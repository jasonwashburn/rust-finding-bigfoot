use rocket::config::Sig;
use serde::{Deserialize, Serialize};
use std::{error::Error, process};
extern crate redis;
use crate::redis::JsonCommands;

const BIGFOOT_DATA_FILE_PATH: &str = "data/bfro_reports_geocoded.csv";
const REDIS_CONNECT_STRING: &str = "redis://127.0.0.1/";

#[derive(Debug, Deserialize, Serialize)]
struct Sighting {
    observed: Option<String>,
    location_details: Option<String>,
    county: String,
    state: String,
    title: Option<String>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    date: Option<String>,
    number: f64,
    classification: String,
    geohash: Option<String>,
    temperature_high: Option<f64>,
    temperature_mid: Option<f64>,
    temperature_low: Option<f64>,
    dew_point: Option<f64>,
    humidity: Option<f64>,
    cloud_cover: Option<f64>,
    moon_phase: Option<f64>,
    precip_intensity: Option<f64>,
    precip_probability: Option<f64>,
    precip_type: Option<String>,
    pressure: Option<f64>,
    summary: Option<String>,
    uv_index: Option<f64>,
    visibility: Option<f64>,
    wind_bearing: Option<f64>,
    wind_speed: Option<f64>,
}

#[rocket::main]
async fn main() -> Result<(), rocket::Error> {
    let sightings = match load_data() {
        Ok(sightings) => sightings,
        Err(e) => {
            println!("{}", e);
            process::exit(1);
        }
    };

    if let Err(err) = write_to_redis(sightings) {
        println!("{}", err);
        process::exit(1);
    }

    let _rocket = rocket::build()
        .mount("/", routes![index, sightings])
        .launch()
        .await?;

    Ok(())
}

#[macro_use]
extern crate rocket;

#[get("/")]
fn index() -> &'static str {
    "Hello, world!"
}

#[get("/sightings/<id>")]
fn sightings(id: i64) -> String {
    let client = redis::Client::open(REDIS_CONNECT_STRING).unwrap();
    let mut con = client.get_connection().unwrap();
    let key = format!("sighting:{id}");
    println!("{}", &key);
    let sighting: String = con.json_get(key, "$").unwrap();
    let sighting = sighting
        .strip_prefix("[")
        .unwrap()
        .strip_suffix("]")
        .unwrap();
    dbg!(&sighting);
    let sighting: Sighting = serde_json::from_str(sighting).unwrap();
    serde_json::to_string(&sighting).unwrap()
}

fn load_data() -> Result<Vec<Sighting>, Box<dyn Error>> {
    let mut results: Vec<Sighting> = Vec::new();
    let mut reader = csv::Reader::from_path(BIGFOOT_DATA_FILE_PATH)?;
    for result in reader.deserialize() {
        let record: Sighting = result?;
        results.push(record)
    }
    Ok(results)
}

fn write_to_redis(sightings: Vec<Sighting>) -> redis::RedisResult<()> {
    let start = std::time::Instant::now();

    let client = redis::Client::open(REDIS_CONNECT_STRING)?;

    let mut con = client.get_connection()?;
    for record in sightings.iter() {
        let idx = record.number;
        let json = serde_json::to_string(&record).unwrap();
        redis::cmd("JSON.SET")
            .arg(format!("sighting:{idx}"))
            .arg("$")
            .arg(&json)
            .query(&mut con)?;
    }
    let num_records = sightings.len();
    let duration = start.elapsed();
    println!("Loaded {num_records} records in {:?} seconds", duration);
    Ok(())
}
