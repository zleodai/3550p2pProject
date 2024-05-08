use libp2p::{
    core::upgrade,
    floodsub::{Floodsub, FloodsubEvent, Topic},
    futures::StreamExt,
    identity,
    mdns::{Mdns, MdnsEvent},
    mplex,
    noise::{Keypair, NoiseConfig, X25519Spec},
    swarm::{NetworkBehaviourEventProcess, Swarm, SwarmBuilder},
    tcp::TokioTcpConfig,
    NetworkBehaviour, PeerId, Transport,
};
use log::{error, info};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tokio::{fs, io::AsyncBufReadExt, sync::mpsc};
use rand::prelude::*;
use async_std::fs::File;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;
type CardsInHand = Vec<Card>;

static KEYS: Lazy<identity::Keypair> = Lazy::new(|| identity::Keypair::generate_ed25519());
static PEER_ID: Lazy<PeerId> = Lazy::new(|| PeerId::from(KEYS.public()));
static TOPIC: Lazy<Topic> = Lazy::new(|| Topic::new("articles"));

static PATH_NAME: Lazy<String> = Lazy::new(|| format!("./Cards_In_Hand_{}.json", PEER_ID.to_string()));

#[derive(Debug, Serialize, Deserialize)]
struct Card {
    id: usize,
    name: String,
    revealed: bool,
}

#[derive(Debug, Serialize, Deserialize)]
enum ListMode {
    ALL,
    One(String),
}

#[derive(Debug, Serialize, Deserialize)]
struct ListRequest {
    mode: ListMode,
}

#[derive(Debug, Serialize, Deserialize)]
struct ListResponse {
    mode: ListMode,
    data: CardsInHand,
    receiver: String,
}

enum EventType {
    Response(ListResponse),
    Input(String),
}

#[derive(NetworkBehaviour)]
struct CardBehavior {
    floodsub: Floodsub,
    mdns: Mdns,
    #[behaviour(ignore)]
    response_sender: mpsc::UnboundedSender<ListResponse>,
}

impl NetworkBehaviourEventProcess<FloodsubEvent> for CardBehavior {
    fn inject_event(&mut self, event: FloodsubEvent) {
        match event {
            FloodsubEvent::Message(msg) => {
                if let Ok(resp) = serde_json::from_slice::<ListResponse>(&msg.data) {
                    if resp.receiver == PEER_ID.to_string() {
                        info!("Response from {}:", msg.source);
                        resp.data.iter().for_each(|r| info!("{:?}", r));
                    }
                } else if let Ok(req) = serde_json::from_slice::<ListRequest>(&msg.data) {
                    match req.mode {
                        ListMode::ALL => {
                            info!("Received ALL req: {:?} from {:?}", req, msg.source);
                            respond_with_public_cards(
                                self.response_sender.clone(),
                                msg.source.to_string(),
                            );
                        }
                        ListMode::One(ref peer_id) => {
                            if peer_id == &PEER_ID.to_string() {
                                info!("Received req: {:?} from {:?}", req, msg.source);
                                respond_with_public_cards(
                                    self.response_sender.clone(),
                                    msg.source.to_string(),
                                );
                            }
                        }
                    }
                }
            }
            _ => (),
        }
    }
}

fn respond_with_public_cards(sender: mpsc::UnboundedSender<ListResponse>, receiver: String) {
    tokio::spawn(async move {
        match read_local_cards().await {
            Ok(cards) => {
                let resp = ListResponse {
                    mode: ListMode::ALL,
                    receiver,
                    data: cards.into_iter().filter(|r| r.revealed).collect(),
                };
                if let Err(e) = sender.send(resp) {
                    error!("error sending response via channel, {}", e);
                }
            }
            Err(e) => error!("error fetching local cards to answer ALL request, {}", e),
        }
    });
}


impl NetworkBehaviourEventProcess<MdnsEvent> for CardBehavior {
    fn inject_event(&mut self, event: MdnsEvent) {
        match event {
            MdnsEvent::Discovered(discovered_list) => {
                for (peer, _addr) in discovered_list {
                    self.floodsub.add_node_to_partial_view(peer);
                }
            }
            MdnsEvent::Expired(expired_list) => {
                for (peer, _addr) in expired_list {
                    if !self.mdns.has_node(&peer) {
                        self.floodsub.remove_node_from_partial_view(&peer);
                    }
                }
            }
        }
    }
}

async fn create_new_card(name: &str) -> Result<()> {
    let mut local_cards = read_local_cards().await?;
    let mut rng = rand::thread_rng();
    let random_id: f64 = rng.gen();
    let mut floored_id = (random_id * 52.0) as i64;
    if floored_id < 0 {
        floored_id = 0
    } else if floored_id >= 52 {
        floored_id = 51
    }
    let input_id = floored_id as usize;

    local_cards.push(Card {
        id: input_id,
        name: name.to_owned(),
        revealed: false,
    });
    write_local_cards(&local_cards).await?;

    info!("Added Card {}", name);

    Ok(())
}

async fn reveal_card(id: usize) -> Result<()> {
    let mut local_cards = read_local_cards().await?;
    local_cards
        .iter_mut()
        .filter(|r| r.id == id)
        .for_each(|r| r.revealed = true);
    write_local_cards(&local_cards).await?;
    Ok(())
}

async fn read_local_cards() -> Result<CardsInHand> {
    let STORAGE_FILE_PATH: &str = &*PATH_NAME;
    let content = fs::read(STORAGE_FILE_PATH).await?;
    let result = serde_json::from_slice(&content)?;
    Ok(result)
}

async fn write_local_cards(cards: &CardsInHand) -> Result<()> {
    let STORAGE_FILE_PATH: &str = &*PATH_NAME;
    let json = serde_json::to_string(&cards)?;
    fs::write(STORAGE_FILE_PATH, json).await?;
    Ok(())
}

async fn create_local_cards(cards: &CardsInHand) -> Result<()> {
    let STORAGE_FILE_PATH: &str = &*PATH_NAME;
    let json = serde_json::to_string(&cards)?;
    let _file = File::create(STORAGE_FILE_PATH).await?;
    fs::write(STORAGE_FILE_PATH, json).await?;
    Ok(())
}

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "info");
    pretty_env_logger::init();

    let _ = start_up_game().await;

    info!("Peer Id: {}", PEER_ID.clone());
    let (response_sender, mut response_rcv) = mpsc::unbounded_channel();

    let auth_keys = Keypair::<X25519Spec>::new()
        .into_authentic(&KEYS)
        .expect("can create auth keys");

    let transp = TokioTcpConfig::new()
        .upgrade(upgrade::Version::V1)
        .authenticate(NoiseConfig::xx(auth_keys).into_authenticated()) // XX Handshake pattern, IX exists as well and IK - only XX currently provides interop with other libp2p impls
        .multiplex(mplex::MplexConfig::new())
        .boxed();

    let mut behaviour = CardBehavior {
        floodsub: Floodsub::new(PEER_ID.clone()),
        mdns: Mdns::new(Default::default())
            .await
            .expect("can create mdns"),
        response_sender,
    };

    behaviour.floodsub.subscribe(TOPIC.clone());

    let mut swarm = SwarmBuilder::new(transp, behaviour, PEER_ID.clone())
        .executor(Box::new(|fut| {
            tokio::spawn(fut);
        }))
        .build();

    let mut stdin = tokio::io::BufReader::new(tokio::io::stdin()).lines();

    Swarm::listen_on(
        &mut swarm,
        "/ip4/0.0.0.0/tcp/0"
            .parse()
            .expect("can get a local socket"),
    )
    .expect("swarm can be started");

    loop {
        let evt = {
            tokio::select! {
                line = stdin.next_line() => Some(EventType::Input(line.expect("can get line").expect("can read line from stdin"))),
                response = response_rcv.recv() => Some(EventType::Response(response.expect("response exists"))),
                event = swarm.select_next_some() => {
                    //info!("Unhandled Swarm Event: {:?}", event);
                    None
                },
            }
        };

        if let Some(event) = evt {
            match event {
                EventType::Response(resp) => {
                    let json = serde_json::to_string(&resp).expect("can jsonify response");
                    swarm
                        .behaviour_mut()
                        .floodsub
                        .publish(TOPIC.clone(), json.as_bytes());
                }
                EventType::Input(line) => match line.as_str() {
                    "ls p" => handle_list_peers(&mut swarm).await,
                    cmd if cmd.starts_with("ls b") => handle_list_cards(cmd, &mut swarm).await,
                    cmd if cmd.starts_with("create b") => handle_create_card(cmd).await,
                    cmd if cmd.starts_with("reveal b") => handle_reveal_card(cmd).await,
                    _ => error!("unknown command"),
                },
            }
        }
    }
}

async fn handle_list_peers(swarm: &mut Swarm<CardBehavior>) {
    info!("Discovered Peers:");
    let nodes = swarm.behaviour().mdns.discovered_nodes();
    let mut unique_peers = HashSet::new();
    for peer in nodes {
        unique_peers.insert(peer);
    }
    unique_peers.iter().for_each(|p| info!("{}", p));
}

async fn handle_list_cards(cmd: &str, swarm: &mut Swarm<CardBehavior>) {
    let rest = cmd.strip_prefix("ls r");
    match rest {
        Some("all") => {
            let req = ListRequest {
                mode: ListMode::ALL,
            };
            let json = serde_json::to_string(&req).expect("can jsonify request");
            swarm
                .behaviour_mut()
                .floodsub
                .publish(TOPIC.clone(), json.as_bytes());
        }
        Some(card_peer_id) => {
            let req = ListRequest {
                mode: ListMode::One(card_peer_id.to_owned()),
            };
            let json = serde_json::to_string(&req).expect("can jsonify request");
            swarm
                .behaviour_mut()
                .floodsub
                .publish(TOPIC.clone(), json.as_bytes());
        }
        None => {
            match read_local_cards().await {
                Ok(v) => {
                    info!("Local Cards ({})", v.len());
                    v.iter().for_each(|r| info!("{:?}", r));
                }
                Err(e) => error!("error fetching local cards: {}", e),
            };
        }
    };
}

async fn handle_create_card(cmd: &str) {
    if let Some(rest) = cmd.strip_prefix("create b ") {
        let elements: Vec<&str> = rest.split("|").collect();
        if elements.len() == 0 {
            info!("too few arguments - Format: name");
        } else {
            let name = elements.get(0).expect("card is there");
            if let Err(e) = create_new_card(name).await {
                error!("error creating card: {}", e);
            };
        }
    }
}

async fn handle_reveal_card(cmd: &str) {
    if let Some(rest) = cmd.strip_prefix("reveal b ") {
        match rest.trim().parse::<usize>() {
            Ok(id) => {
                if let Err(e) = reveal_card(id).await {
                    info!("error publishing card with id {}, {}", id, e)
                } else {
                    info!("Revealed Card with id: {}", id);
                }
            }
            Err(e) => error!("invalid id: {}, {}", rest.trim(), e),
        };
    }
}

async fn start_up_game() -> Result<()> {
    let mut local_cards = CardsInHand::new();
    let mut rng = rand::thread_rng();
    let mut random_id: f64 = rng.gen();
    let mut floored_id = (random_id * 52.0) as i64;
    if floored_id >= 52 {
        floored_id = 51
    }
    let mut input_id = floored_id as usize;
    let name: String = get_card_string_from_id(floored_id).await?;

    local_cards.push(Card {
        id: input_id,
        name: name.to_owned(),
        revealed: false,
    });

    random_id = rng.gen();
    floored_id = (random_id * 52.0) as i64;
    if floored_id >= 52 {
        floored_id = 51
    }
    input_id = floored_id as usize;
    let name = get_card_string_from_id(floored_id).await?;

    local_cards.push(Card {
        id: input_id,
        name: name.to_owned(),
        revealed: false,
    });

    create_local_cards(&local_cards).await?;
    
    Ok(())
}

async fn get_card_string_from_id(cmd: i64) -> Result<String> {
    let card_id: i64 = cmd.try_into().unwrap();


    Ok("Test".to_string())
}