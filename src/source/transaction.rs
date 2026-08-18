use {
    crate::{
        source::{fees::TransactionFees, sfa::SignatureForAddress},
        storage::rocksdb::TransactionIndex,
        util::{HashMap, HashSet},
    },
    prost::Message as _,
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_storage_proto::convert::generated,
    solana_transaction::TransactionError,
    solana_transaction_status::{TransactionWithStatusMeta, extract_and_fmt_memos},
};

#[derive(Debug)]
pub struct TransactionWithBinary {
    pub key: [u8; 8],
    pub signature: Signature,
    pub is_vote: bool,
    pub err: Option<TransactionError>,
    pub sfa: Vec<SignatureForAddress>,
    pub fees: Option<TransactionFees>,
    pub protobuf: Vec<u8>,
    /// Original transaction index within the block (independent of any vote filtering
    /// applied afterwards to `BlockWithBinary::transactions`).
    pub index: u32,
}

impl TransactionWithBinary {
    pub fn new(
        slot: Slot,
        tx: TransactionWithStatusMeta,
        is_vote: Option<bool>,
        transaction_index: u32,
    ) -> Self {
        let signature = *tx.transaction_signature();
        let key = TransactionIndex::encode(&signature);

        let (err, sfa) = match &tx {
            TransactionWithStatusMeta::MissingMetadata(_) => (None, vec![]),
            TransactionWithStatusMeta::Complete(tx) => {
                let account_keys = tx.account_keys();
                let err = tx.meta.status.clone().err();
                let memo = extract_and_fmt_memos(tx);
                let mut sfa = Vec::with_capacity(account_keys.len());
                for pubkey in account_keys.iter() {
                    sfa.push(SignatureForAddress::new(
                        slot,
                        *pubkey,
                        signature,
                        err.clone(),
                        memo.clone(),
                        transaction_index,
                    ))
                }

                let direct_keys: HashSet<&Pubkey> = account_keys.iter().collect();

                let pre = tx.meta.pre_token_balances.as_deref().unwrap_or(&[]);
                let post = tx.meta.post_token_balances.as_deref().unwrap_or(&[]);

                let pre_amounts: HashMap<u8, &str> = pre
                    .iter()
                    .map(|b| (b.account_index, b.ui_token_amount.amount.as_str()))
                    .collect();
                let post_amounts: HashMap<u8, &str> = post
                    .iter()
                    .map(|b| (b.account_index, b.ui_token_amount.amount.as_str()))
                    .collect();

                let mut owner_balance_changed: HashSet<&str> = HashSet::default();
                for b in pre.iter().chain(post.iter()) {
                    let pre_amt = pre_amounts.get(&b.account_index).copied();
                    let post_amt = post_amounts.get(&b.account_index).copied();
                    if pre_amt != post_amt {
                        owner_balance_changed.insert(b.owner.as_str());
                    }
                }

                let mut seen_owners: HashSet<&str> = HashSet::default();
                for b in pre.iter().chain(post.iter()) {
                    if !seen_owners.insert(b.owner.as_str()) {
                        continue;
                    }
                    let Ok(owner_pubkey) = b.owner.parse::<Pubkey>() else {
                        continue;
                    };
                    if direct_keys.contains(&owner_pubkey) {
                        continue;
                    }
                    let balance_changed = owner_balance_changed.contains(b.owner.as_str());
                    sfa.push(SignatureForAddress::new_token_owner(
                        slot,
                        owner_pubkey,
                        signature,
                        err.clone(),
                        memo.clone(),
                        transaction_index,
                        balance_changed,
                    ));
                }

                (err, sfa)
            }
        };

        let is_vote = match (is_vote, &tx) {
            (Some(v), _) => v,
            (None, TransactionWithStatusMeta::Complete(tx)) => TransactionFees::is_vote(tx),
            (None, TransactionWithStatusMeta::MissingMetadata(_)) => false,
        };

        let fees = TransactionFees::create(&tx, Some(is_vote));

        let protobuf = generated::ConfirmedTransaction::from(tx).encode_to_vec();

        Self {
            key,
            signature,
            is_vote,
            err,
            sfa,
            fees,
            protobuf,
            index: transaction_index,
        }
    }
}
