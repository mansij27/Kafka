use kafka::producer::{Producer, Record};
use csv::Reader;
use std::error::Error;

fn read_csv(file_path: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let mut rdr = Reader::from_path("/home/knoldus/kafka/kafka_producer/src/sensor.csv")?;
    let mut records = Vec::new();
    for result in rdr.records() {
        let record = result?;
        records.push(record.iter().collect::<Vec<&str>>().join(","));
    }
    Ok(records)
}

fn main() {
    let hosts = vec!["localhost:9092".to_owned()];
    let mut producer = Producer::from_hosts(hosts).create().unwrap();

    match read_csv("/home/knoldus/kafka/kafka_producer/src/sensor.csv") {
        Ok(records) => {
            for record in records {
                let buf = format!("{:#?}", record);
                producer.send(&Record::from_value("texts", buf.as_bytes())).unwrap();
                println!("Sent: {:#?}", record);
            }
        }
        Err(e) => println!("Error reading CSV file: {}", e),
    }
}

//   for i in 0..20 {
//     let buf = format!("{i}");
//     producer.send(&Record::from_value("texts", buf.as_bytes())).unwrap();
//     println!("Sent: {i}");
//   }
// }
