use wincode::{SchemaRead, SchemaWrite};


#[derive(Debug, Clone, SchemaRead, SchemaWrite)]
pub enum Signal {
    ClickSnapshot { timestamp: i64 },
    ReplaySnapshot {
        recording_id: i32,
        start_seq_num: i32,
        end_seq_num: i32,
        snapshot: Vec<u8>,
    },
}


// input: we need to send via websocket to matching engine for snapshot.

// #[derive(SchemaRead, SchemaWrite)]
// enum Signal {
//     ClickSnapshot { 
//         timestamp: i64,
//     },
//     ReplaySnapshot { 
//         recording_id: i32, 
//         start_seq_num: i32, 
//         end_seq_num: i32, 
//         snapshot: Vec<u8>
//     }
// }

// Output: we will receive from websocket from order matching engine. 
// Vec<u8>
// we need to compare and show the result. 

