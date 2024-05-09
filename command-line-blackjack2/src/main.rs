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
use async_std::fs::File as Async_File;
use std::fs::File;
use std::fs as regular_fs;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;
type CardsInHand = Vec<Card>;

static KEYS: Lazy<identity::Keypair> = Lazy::new(|| identity::Keypair::generate_ed25519());
static PEER_ID: Lazy<PeerId> = Lazy::new(|| PeerId::from(KEYS.public()));
static TOPIC: Lazy<Topic> = Lazy::new(|| Topic::new("articles"));

static PATH_NAME: Lazy<String> = Lazy::new(|| format!("./Cards_In_Hand_{}.json", PEER_ID.to_string()));
static ENEMY_PATH_NAME: Lazy<String> = Lazy::new(|| format!("./Enemy_Visible_Cards{}.json", PEER_ID.to_string()));
static STATUS_PATH: Lazy<String> = Lazy::new(|| format!("./Status_{}.json", PEER_ID.to_string()));
static ENEMY_STATUS_PATH: Lazy<String> = Lazy::new(|| format!("./Enemy_Status_{}.json", PEER_ID.to_string()));
static ENEMY_FOUND_PATH: Lazy<String> = Lazy::new(|| format!("./Enemy_Found{}.json", PEER_ID.to_string()));
static STAND_PATH: Lazy<String> = Lazy::new(|| format!("./Stand{}.json", PEER_ID.to_string()));
static ENEMY_STAND_PATH: Lazy<String> = Lazy::new(|| format!("./Enemy_Stand{}.json", PEER_ID.to_string()));

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
    status: i64,
    stand: i64,
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
                        update_enemy_cards(&resp.data);
                        update_enemy_status_json(resp.status);
                        update_enemy_found_json(1);
                        update_enemy_stand_json(resp.stand);
                    }
                } else if let Ok(req) = serde_json::from_slice::<ListRequest>(&msg.data) {
                    match req.mode {
                        ListMode::ALL => {
                            //info!("Received ALL req: {:?} from {:?}", req, msg.source);
                            respond_with_public_cards(
                                self.response_sender.clone(),
                                msg.source.to_string(),
                            );
                        }
                        ListMode::One(ref peer_id) => {
                            if peer_id == &PEER_ID.to_string() {
                                //info!("Received req: {:?} from {:?}", req, msg.source);
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
        let score: i64 = read_local_status().await.expect("Get Score");
        let stand: i64 = read_player_stand().await.expect("Get Stand");
        match read_local_cards().await {
            Ok(cards) => {
                let resp = ListResponse {
                    mode: ListMode::ALL,
                    receiver,
                    data: cards.into_iter().filter(|r| r.revealed).collect(),
                    status: score,
                    stand: stand
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

async fn create_new_card() -> Result<()> {
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
    let name = get_card_string_from_id(floored_id).await?;

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
    let storage_file_path: &str = &*PATH_NAME;
    let content = fs::read(storage_file_path).await?;
    let result = serde_json::from_slice(&content)?;
    Ok(result)
}

async fn write_local_cards(cards: &CardsInHand) -> Result<()> {
    let storage_file_path: &str = &*PATH_NAME;
    let json = serde_json::to_string(&cards)?;
    fs::write(storage_file_path, json).await?;
    Ok(())
}

async fn create_new_cards_json() -> Result<()> {
    let storage_file_path: &str = &*PATH_NAME;
    let _file = Async_File::create(storage_file_path).await?;
    fs::write(storage_file_path, b"[]").await?;
    Ok(())
}

async fn read_local_status() -> Result<i64> {
    let storage_file_path: &str = &*STATUS_PATH;
    let content = fs::read(storage_file_path).await?;
    let result = serde_json::from_slice(&content)?;
    Ok(result)
}

async fn update_status_json(sum: i64) -> Result<()> {
    let storage_file_path: &str = &*STATUS_PATH;
    let _file = Async_File::create(storage_file_path).await?;
    fs::write(storage_file_path, sum.to_string()).await?;
    Ok(())
}

async fn create_player_stand() -> Result<()> {
    let storage_file_path: &str = &*STAND_PATH;
    let _file = Async_File::create(storage_file_path).await?;
    let json = serde_json::to_string(&0);
    let json_string = json.unwrap_or_else(|err| {
        "default value".to_string()
    });
    regular_fs::write(storage_file_path, json_string);
    Ok(())
}

async fn update_player_stand(x: i64) -> Result<()> {
    let storage_file_path: &str = &*STAND_PATH;
    let _file = Async_File::create(storage_file_path).await?;
    let json = serde_json::to_string(&x);
    let json_string = json.unwrap_or_else(|err| {
        "default value".to_string()
    });
    regular_fs::write(storage_file_path, json_string);
    Ok(())
}

async fn read_player_stand() -> Result<i64> {
    let storage_file_path: &str = &*STAND_PATH;
    let content = fs::read(storage_file_path).await?;
    let result = serde_json::from_slice(&content)?;
    Ok(result)
}

async fn read_local_enemy_status() -> Result<i64> {
    let storage_file_path: &str = &*ENEMY_STATUS_PATH;
    let content = fs::read(storage_file_path).await?;
    let result = serde_json::from_slice(&content)?;
    Ok(result)
}

async fn read_enemy_cards() -> Result<CardsInHand> {
    let storage_file_path: &str = &*ENEMY_PATH_NAME;
    let content = fs::read(storage_file_path).await?;
    let result = serde_json::from_slice(&content)?;
    Ok(result)
}

async fn read_enemy_found() -> Result<i64> {
    let storage_file_path: &str = &*ENEMY_FOUND_PATH;
    let content = fs::read(storage_file_path).await?;
    let result = serde_json::from_slice(&content)?;
    Ok(result)
}

async fn read_enemy_stand() -> Result<i64> {
    let storage_file_path: &str = &*ENEMY_STAND_PATH;
    let content = fs::read(storage_file_path).await?;
    let result = serde_json::from_slice(&content)?;
    Ok(result)
}

fn update_enemy_status_json(sum: i64) {
    let storage_file_path: &str = &*ENEMY_STATUS_PATH;
    let _file = File::create(storage_file_path);
    regular_fs::write(storage_file_path, sum.to_string());
}

fn update_enemy_cards(cards: &CardsInHand) {
    let storage_file_path: &str = &*ENEMY_PATH_NAME;
    let _file = File::create(storage_file_path);
    let json = serde_json::to_string(&cards);
    let json_string = json.unwrap_or_else(|err| {
        "default value".to_string()
    });
    regular_fs::write(storage_file_path, json_string);
}

fn update_enemy_found_json(x: i64){
    let storage_file_path: &str = &*ENEMY_FOUND_PATH;
    let _file = File::create(storage_file_path);
    let json = serde_json::to_string(&x);
    let json_string = json.unwrap_or_else(|err| {
        "default value".to_string()
    });
    regular_fs::write(storage_file_path, json_string);
}

fn update_enemy_stand_json(x: i64){
    let storage_file_path: &str = &*ENEMY_STAND_PATH;
    let _file = File::create(storage_file_path);
    let json = serde_json::to_string(&x);
    let json_string = json.unwrap_or_else(|err| {
        "default value".to_string()
    });
    regular_fs::write(storage_file_path, json_string);
}

async fn create_enemy_cards_json() -> Result<()> {
    let storage_file_path: &str = &*ENEMY_PATH_NAME;
    let _file = File::create(storage_file_path);
    fs::write(storage_file_path, b"[]");
    Ok(())
}

async fn create_enemy_status_json() -> Result<()> {
    let storage_file_path: &str = &*ENEMY_STATUS_PATH;
    let _file = File::create(storage_file_path);
    fs::write(storage_file_path, b"0");
    Ok(())
}

async fn create_enemy_found_json() -> Result<()> {
    let storage_file_path: &str = &*ENEMY_FOUND_PATH;
    let _file = File::create(storage_file_path);
    let json = serde_json::to_string(&0);
    let json_string = json.unwrap_or_else(|err| {
        "default value".to_string()
    });
    regular_fs::write(storage_file_path, json_string);
    Ok(())
}

async fn create_enemy_stand() -> Result<()> {
    let storage_file_path: &str = &*ENEMY_STAND_PATH;
    let _file = File::create(storage_file_path);
    let json = serde_json::to_string(&0);
    let json_string = json.unwrap_or_else(|err| {
        "default value".to_string()
    });
    regular_fs::write(storage_file_path, json_string);
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
        info!("\n\n\n\n\n");

        let evt = {
            tokio::select! {
                line = stdin.next_line() => Some(EventType::Input(line.expect("can get line").expect("can read line from stdin"))),
                response = response_rcv.recv() => Some(EventType::Response(response.expect("response exists"))),
                _event = swarm.select_next_some() => {
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
                    cmd if cmd.starts_with("see cards") => handle_list_cards(cmd, &mut swarm).await,
                    cmd if cmd.starts_with("get card") => handle_create_card().await,
                    cmd if cmd.starts_with("draw card") => handle_create_card().await,
                    cmd if cmd.starts_with("show card") => handle_reveal_card(cmd).await,
                    cmd if cmd.starts_with("restart") => restart_game().await,
                    cmd if cmd.starts_with("start") => handle_ping_enemy(&mut swarm).await,
                    cmd if cmd.starts_with("ping") => handle_ping_enemy(&mut swarm).await,
                    cmd if cmd.starts_with("stand") => handle_stand().await,
                    _ => error!("unknown command"),
                },
            }
        }

        let score = get_score().await.expect("Get Score");
        update_status_json(score).await.expect("Update Score");
        let status = read_local_status().await.expect("Get Status");

        let enemy_found = read_enemy_found().await.expect("Get Enemy Found");
        if enemy_found == 1 {
            info!("Game started");
            
            let mut cards_list = HashSet::new();
            let local_cards = read_local_cards().await.expect("Get Cards"); 

            for card in local_cards{
                cards_list.insert(card.name);
            }

            info!("Your cards: ");

            cards_list.iter().for_each(|card_name| info!("{}", card_name));

            info!("\n\n");

            info!("Enemy visible cards: ");

            let mut enemy_cards_list = HashSet::new();
            let enemy_cards = read_enemy_cards().await.expect("Get Cards"); 

            for card in enemy_cards{
                enemy_cards_list.insert(card.name);
            }

            enemy_cards_list.iter().for_each(|card_name| info!("{}", card_name));

            let blackjack_check: bool = check_if_blackjack().await.expect("Get Blackjack");
            if blackjack_check {
                info!("You won! Type restart to restart the game (Both Players must restart)");
            }
    
            let bust_check: bool = check_if_bust().await.expect("Get Bust");
            if bust_check {
                info!("You busted! Type restart to restart the game (Both Players must restart)");
            }

            let enemy_score: i64 = read_local_enemy_status().await.expect("Get Enemy Score");
            if enemy_score > 21 {
                info!("Enemy Busted! You Win! Type restart to restart the game (Both Players must restart)")
            } else if enemy_score == 21 {
                info!("Enemy got won! Type restart to restart the game (Both Players must restart)")
            }

            let stand_value = read_player_stand().await.expect("Get Stand Value");
            let enemy_stand_value = read_enemy_stand().await.expect("Get Enemy Stand Value");

            let mut stand_check: bool = false;
            if stand_value == 1 && enemy_stand_value == 1{
                stand_check = true
            }

            if !bust_check && !blackjack_check && !stand_check && stand_value == 0 {
                info!("Say 'draw card' to draw a card");
                info!("Or Say 'stand' to stand");
                info!("\n");
            } else if (!bust_check && !blackjack_check && !stand_check) {
                info!("You stand. Waiting for player to make a move")
                info!("Say 'ping' to see enemy player action");
            } else if (stand_check){
                let player_score: i64 = get_score().await.expect("Get Player Score");

                if enemy_score > player_score{
                    info!("Enemy got a score of {}, You got a score of {}\nYOU LOSE!!", enemy_score, player_score);
                    info!("Type restart to restart the game (Both Players must restart)");
                } else if (player_score > enemy_score){
                    info!("Enemy got a score of {}, You got a score of {}\nYOU WIN!!", enemy_score, player_score);
                    info!("Type restart to restart the game (Both Players must restart)");
                } else {
                    info!("Enemy got a score of {}, You got a score of {}\nDRAW!!", enemy_score, player_score);
                    info!("Type restart to restart the game (Both Players must restart)");
                }
            }
        } else {
            info!("Could not find another player\n\n");
            info!("Say 'start' to start the game when another player is connected");
        }
    }
}

async fn handle_stand() {
    update_player_stand(1).await.expect("Updating Stand");
}

async fn handle_ping_enemy(swarm: &mut Swarm<CardBehavior>) {
    let _ = ping_enemy(swarm).await.expect("Pinged Enemy");
    let _ = ping_enemy(swarm).await.expect("Pinged Enemy");
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
    let rest = cmd.strip_prefix("see cards");
    let rest_str = match rest {
        Some(s) => s,
        None => "Default Value",
    };
    info!("Input:{}", rest_str);
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

async fn handle_create_card() {
    if let Err(e) = create_new_card().await {
        error!("error creating card: {}", e);
    };
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

async fn restart_game() {
    let _ = start_up_game().await;
}

async fn start_up_game() -> Result<()> {
    create_new_cards_json().await?;
    create_new_card().await?;
    create_new_card().await?;
    let local_cards = read_local_cards().await?; 
    reveal_card(local_cards[0].id).await?;
    let score = get_score().await?;
    update_status_json(score).await?;

    create_player_stand().await?;
    update_player_stand(0).await?;
    
    create_enemy_cards_json().await.expect("Create Enemy Cards Json");
    create_enemy_status_json().await.expect("Create Enemy Status");
    create_enemy_found_json().await.expect("Create Enemy Found");
    create_enemy_stand().await.expect("Create Enemy Stand");

    Ok(())
}

async fn get_card_string_from_id(cmd: i64) -> Result<String> {
    let mut card_id: i64 = cmd.try_into().unwrap();
    let suit_id: i64 = card_id / 13;
    card_id = card_id % 13;

    let mut card_str: &str = "";
    let mut suit_str: &str = "";

    if card_id == 0 {
        card_str = "Ace ";
    } else if card_id == 1 {
        card_str = "Two ";
    } else if card_id == 2 {
        card_str = "Three ";
    } else if card_id == 3 {
        card_str = "Four ";
    } else if card_id == 4 {
        card_str = "Five ";
    } else if card_id == 5 {
        card_str = "Six ";
    } else if card_id == 6 {
        card_str = "Seven ";
    } else if card_id == 7 {
        card_str = "Eight ";
    } else if card_id == 8 {
        card_str = "Nine ";
    } else if card_id == 9 {
        card_str = "Ten ";
    } else if card_id == 10 {
        card_str = "Jack ";
    } else if card_id == 11 {
        card_str = "Queen ";
    } else {
        card_str = "King ";
    }

    if suit_id == 0 {
        suit_str = "Spades";
    } else if suit_id == 1 {
        suit_str = "Cloves";
    } else if suit_id == 2 {
        suit_str = "Hearts";
    } else {
        suit_str = "Diamonds";
    }

    let card_name: String = card_str.to_owned() + suit_str;

    Ok(card_name)
}

async fn get_card_value_from_id(cmd: i64) -> Result<i64> {
    let mut card_id: i64 = cmd.try_into().unwrap();
    card_id = card_id % 13;

    Ok(card_id)
}

async fn check_if_blackjack() -> Result<bool> {
    let local_cards = read_local_cards().await?; 
    let mut total_sum: i64 = 0;
    let mut ace_count: i64 = 0;
    for item in local_cards{
        let mut card_value = get_card_value_from_id(item.id as i64).await?;
        if card_value >= 10 {
            card_value = 10;
        } else if card_value == 0{
            ace_count += 1;
            card_value += 1;
        } else {
            card_value += 1;
        }
        total_sum += card_value;
    }

    let mut success: bool = false;
    if total_sum == 21 {
        success = true;
    }
    while ace_count > 0 {
        total_sum += 10;
        if total_sum == 21{
            ace_count = 0;
            success = true;
        }
        ace_count -= 1;
    }

    Ok(success)
}

async fn check_if_bust() -> Result<bool> {
    let local_cards = read_local_cards().await?; 
    let mut total_sum: i64 = 0;
    for item in local_cards{
        let mut card_value = get_card_value_from_id(item.id as i64).await?;
        if card_value >= 10 {
            card_value = 10;
        } else if card_value == 0{
            card_value = 1;
        } else {
            card_value += 1;
        }
        total_sum += card_value;
    }

    let mut bust: bool = false;
    if total_sum > 21 {
        bust = true;
    }
    Ok(bust)
}

async fn get_score() -> Result<i64> {
    let local_cards = read_local_cards().await?; 
    let mut total_sum: i64 = 0;
    let mut ace_count: i64 = 0;
    for item in local_cards{
        let mut card_value = get_card_value_from_id(item.id as i64).await?;
        if card_value >= 10 {
            card_value = 10;
        } else if card_value == 0{
            ace_count += 1;
            card_value += 1;
        } else {
            card_value += 1;
        }
        total_sum += card_value;
    }

    while ace_count > 0 && total_sum <= 11{
        total_sum += 10;
        if total_sum == 21{
            ace_count = 0;
        }
        ace_count -= 1;
    }
    
    Ok(total_sum)
}

async fn ping_enemy(swarm: &mut Swarm<CardBehavior>) -> Result<()> {
    let nodes = swarm.behaviour().mdns.discovered_nodes();
    let unique_peers: HashSet<_> = nodes.map(|&peer| peer.clone()).collect();
    unique_peers.iter().for_each(|peer| {
        let req = ListRequest {
            mode: ListMode::One(peer.to_string().to_owned()),
        };
        let json = serde_json::to_string(&req).expect("can jsonify request");
        swarm
            .behaviour_mut()
            .floodsub
            .publish(TOPIC.clone(), json.as_bytes());
    });
    Ok(())
}