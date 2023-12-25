mod pb;
mod abi;
use substreams_database_change::tables::Tables;

use substreams::{Hex, hex};
use abi::erc20::events::Transfer;
use substreams_ethereum::pb::eth::v2 as eth;
use substreams_database_change::pb::database::{ DatabaseChanges};
use substreams::scalar::BigInt;
use pb::erc20::types::v1::{  BalanceChange, BalanceChanges, BalanceChangeType
};
use substreams_ethereum::pb::eth::v2::{Block, Call, TransactionTrace, TransactionTraceStatus};
use prost_types::Timestamp;
use substreams::log::info;
use std::collections::HashMap;
use substreams_ethereum::Event;


const NULL_ADDRESS: [u8; 20] = hex!("0000000000000000000000000000000000000000");
const ZERO_STORAGE_PREFIX: [u8; 16] = hex!("00000000000000000000000000000000");



//create token entity
fn add_tokens_info_entity(tables: &mut Tables, balance_changes: &[BalanceChange], block_number:  &u64) {
    for balance in balance_changes.iter() {
        tables
        .create_row("tokens",  block_number.to_string())
        .set("owner",  balance.owner.clone())
        .set("new_balance",   balance.new_balance.clone())
        .set("transaction_hash",  balance.change_type.clone());  
    }
   
}

 // main db_out function
#[substreams::handlers::map]
fn db_out(
    blk: eth::Block
) -> Result<DatabaseChanges, substreams::errors::Error> {
    let mut balance_changes = Vec::new();
    let block_number = &blk.number;
    let mut tables = Tables::new();
   
    for trx in &blk.transaction_traces{
        
        if trx.status == TransactionTraceStatus::Succeeded as i32 {
        for call in trx.calls.iter() {
            if call.state_reverted {
                continue;
            }

            for log in call.logs.iter() {
                let transfer = match Transfer::match_and_decode(log) {
                    Some(transfer) => transfer,
                    None => continue,
                };

                if transfer.value.is_zero() {
                    continue;
                }
                //TODO PUT BACK IN
                if transfer.from == NULL_ADDRESS {
                    continue;
                }

                // Trying with algorithm #1
                let mut found_balance_changes =
                    find_erc20_balance_changes_algorithm1(trx, call, &transfer);
                if !found_balance_changes.is_empty() {
                    balance_changes.extend(found_balance_changes);
                    continue;
                }

                // No balance changes found using algorithm #1, trying with algorithm #2
                found_balance_changes = find_erc20_balance_changes_algorithm2(&transfer, &call, trx);
                if !found_balance_changes.is_empty() {
                    balance_changes.extend(found_balance_changes);
                    continue;
                }

                // No algorithm could extract the balance change, old/new balance is fixed at 0
                balance_changes.push(BalanceChange {
                    contract: Hex::encode(&call.address),
                    owner: Hex::encode(&transfer.to),
                    new_balance: "0".to_string(),
                    change_type: base_64_to_hex(&trx.hash),
                });
            }
        }
       
    }
    }
    
    // for balance in balance_changes.iter() {
        // if balance.contract.to_lowercase().starts_with("56a86d648c435dc707c8405b78e2ae8eb4e60ba4"){
            add_tokens_info_entity(&mut tables, &balance_changes, &block_number);
        // }
       
    // } 
    Ok(tables.to_database_changes())
}


/// normal case
fn find_erc20_balance_changes_algorithm1(
    trx: &TransactionTrace,
    call: &Call,
    transfer: &Transfer,
) -> Vec<BalanceChange> {
    let mut out = Vec::new();
    let mut keccak_address_map: Option<StorageKeyToAddressMap> = None;

    for storage_change in &call.storage_changes {
        let old_balance = BigInt::from_signed_bytes_be(&storage_change.old_value);
        let new_balance = BigInt::from_signed_bytes_be(&storage_change.new_value);

        let balance_change = new_balance - old_balance;
        let balance_change_abs = if balance_change < BigInt::zero() {
            balance_change.neg()
        } else {
            balance_change
        };

        let value = transfer.value.clone();
        let transfer_value_abs = if value.clone() < BigInt::zero() {
            value.neg()
        } else {
            value.clone()
        };

        if balance_change_abs != transfer_value_abs {
            info!("Balance change does not match transfer value. Balance change: {}, transfer value: {}", balance_change_abs, transfer_value_abs);
            continue;
        }

        // We memoize the keccak address map by call because it is expensive to compute
        if keccak_address_map.is_none() {
            keccak_address_map = Some(erc20_addresses_for_storage_keys(call));
        }

        let keccak_address = match keccak_address_map
            .as_ref()
            .unwrap()
            .get(&storage_change.key)
        {
            Some(address) => address,
            None => {
                if storage_change.key[0..16] == ZERO_STORAGE_PREFIX {
                    info!("Skipping balance change for zero key");
                    continue;
                }

                info!(
                    "No keccak address found for key: {}, trx {}",
                    Hex(&storage_change.key),
                    Hex(&trx.hash)
                );
                continue;
            }
        };

        if !erc20_is_valid_address(keccak_address, transfer) {
            info!("Keccak address does not match transfer address. Keccak address: {}, sender address: {}, receiver address: {}, trx {}", Hex(keccak_address), Hex(&transfer.from), Hex(&transfer.to), Hex(&trx.hash));
            continue;
        }

        let change = BalanceChange {
            contract: Hex::encode(&storage_change.address),
            owner: Hex::encode(keccak_address),
            new_balance: BigInt::from_signed_bytes_be(&storage_change.new_value).to_string(),
            change_type: base_64_to_hex(&trx.hash)
        };

        out.push(change);
    }

    out
}

// case where storage changes are not in the same call as the transfer event
fn find_erc20_balance_changes_algorithm2(
    transfer: &Transfer,
    original_call: &Call,
    trx: &TransactionTrace,
) -> Vec<BalanceChange> {
    let mut out = Vec::new();

    //get all keccak keys for transfer.to and transfer.from

    let mut keys = HashMap::new();
    for call in trx.calls.iter() {
        let keccak_address_map = erc20_addresses_for_storage_keys(call);
        keys.extend(keccak_address_map);
    }

    let child_calls = get_all_child_calls(original_call, trx);

    //get all storage changes for these calls:
    let mut storage_changes = Vec::new();
    for call in child_calls.iter() {
        storage_changes.extend(call.storage_changes.clone());
    }

    let mut total_sent = BigInt::zero();
    let mut total_received = BigInt::zero();

    //check if any of the storage changes match the transfer.to or transfer.from
    for storage_change in storage_changes.clone().iter() {
        let keccak_address = match keys.get(&storage_change.key) {
            Some(address) => address,
            None => continue,
        };

        if !erc20_is_valid_address(keccak_address, transfer) {
            continue;
        }

        let old_balance = BigInt::from_signed_bytes_be(&storage_change.old_value);
        let new_balance = BigInt::from_signed_bytes_be(&storage_change.new_value);

        let balance_change = new_balance - old_balance;
        if balance_change < BigInt::zero() {
            total_sent = total_sent + balance_change.neg();
        } else {
            total_received = total_received + balance_change;
        };

        let change = BalanceChange {
            contract: Hex::encode(&storage_change.address),
            owner: Hex::encode(keccak_address),
            new_balance: BigInt::from_signed_bytes_be(&storage_change.new_value).to_string(),
            change_type: base_64_to_hex(&trx.hash),
        };

        out.push(change);
    }

    if total_sent == transfer.value {
        return out;
    }

    let mut diff = total_sent - total_received;
    if diff < BigInt::zero() {
        diff = diff.neg();
    }

    //look for a storage change that matches the diff
    for storage_change in storage_changes.iter() {
        let keccak_address = match keys.get(&storage_change.key) {
            Some(address) => address,
            None => continue,
        };

        let old_balance = BigInt::from_signed_bytes_be(&storage_change.old_value);
        let new_balance = BigInt::from_signed_bytes_be(&storage_change.new_value);

        let mut balance_change = new_balance - old_balance;
        if balance_change < BigInt::zero() {
            balance_change = balance_change.neg();
        }

        if balance_change != diff {
            continue;
        }

        let change = BalanceChange {
            contract: Hex::encode(&storage_change.address),
            owner: Hex::encode(keccak_address),
            new_balance: BigInt::from_signed_bytes_be(&storage_change.new_value).to_string(),
            change_type: base_64_to_hex(&trx.hash),
        };

        out.push(change);
    }

    out
}

type StorageKeyToAddressMap = HashMap<Vec<u8>, Vec<u8>>;

fn erc20_addresses_for_storage_keys(call: &Call) -> StorageKeyToAddressMap {
    let mut out = HashMap::new();

    for (hash, preimage) in &call.keccak_preimages {
        if preimage.len() != 128 {
            continue;
        }

        if &preimage[64..126] != "00000000000000000000000000000000000000000000000000000000000000" {
            continue;
        }

        let addr = &preimage[24..64];
        out.insert(
            Hex::decode(hash).expect("Failed to decode hash hex string"),
            Hex::decode(addr).expect("Failed to decode address hex string"),
        );
    }

    out
}

fn erc20_is_valid_address(address: &Vec<u8>, transfer: &Transfer) -> bool {
    address == &transfer.from || address == &transfer.to
}

fn get_all_child_calls(original: &Call, trx: &TransactionTrace) -> Vec<Call> {
    let mut out = Vec::new();

    for call in trx.calls.iter() {
        if call.parent_index == original.index {
            out.push(call.clone());
        }
    }
    out
}

fn base_64_to_hex<T: std::convert::AsRef<[u8]>>(num:T) -> String {
    let num = hex::encode(&num);
    let num = num.to_string();
     format!("0x{}", &num)
}
