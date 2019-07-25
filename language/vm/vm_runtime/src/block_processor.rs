// Copyright (c) The Libra Core Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    code_cache::{
        module_adapter::ModuleFetcherImpl,
        module_cache::{BlockModuleCache, ModuleCache, VMModuleCache},
        script_cache::ScriptCache,
    },
    counters::{report_block_count, report_execution_status},
    data_cache::BlockDataCache,
    parallel_execution::DAGBoundaries,
    process_txn::{execute::ExecutedTransaction, validate::ValidationMode, ProcessTransaction},
    runtime::Execute,
};
use config::config::VMPublishingOption;
use logger::prelude::*;
use rayon::prelude::*;
use sequence_trie::SequenceTrie;
use state_view::StateView;
use std::collections::{vec_deque::VecDeque, HashMap, HashSet};
use types::{
    access_path::AccessPath,
    account_address::AccountAddress,
    transaction::{
        SignatureCheckedTransaction, SignedTransaction, TransactionOutput, TransactionStatus,
    },
    vm_error::{ExecutionStatus, VMStatus, VMValidationStatus},
    write_set::WriteSet,
};
use vm_cache_map::Arena;

// creates a 2d vector of transaction sequence numbers, where each element of the
// vector represent the topological levels of the dag
pub fn process_dag(
    signature_check_passed_txns: Vec<(usize, SignatureCheckedTransaction)>,
) -> DAGBoundaries {
    let mut boundaries = DAGBoundaries::new();
    let mut senders = HashMap::new();
    for (index, txn) in signature_check_passed_txns {
        let txn_sender = txn.sender();
        let level = *senders
            .entry(txn_sender)
            .and_modify(|depth| *depth += 1)
            .or_insert(0);
        boundaries.insert(level, (index, txn));
    }
    boundaries
}

pub fn get_account_path_map(write_set: &WriteSet) -> HashMap<AccountAddress, SequenceTrie<u8, ()>> {
    let mut account_map = HashMap::new();
    for (ap, _) in write_set {
        let mut path_trie = SequenceTrie::new();
        let account = ap.address;
        let ref path = ap.path;
        path_trie.insert(path, ());
        account_map.insert(account, path_trie);
    }
    account_map
}

pub fn find_intersection(
    access_path: &AccessPath,
    account_map: &HashMap<AccountAddress, SequenceTrie<u8, ()>>,
) -> bool {
    let compare_account = access_path.address;
    let ref compare_path = access_path.path;
    let is_intersecting = match account_map.get(&compare_account) {
        Some(trie) => ((*trie.get_prefix_nodes(compare_path)).len() > 0),
        None => false,
    };
    is_intersecting
}

// Two transactions with accesspaths add1/x/y and add1/x are dependent.
// We use a sequenceTrie data structure to find the dependencies between transactions.
pub fn determine_dependency(
    txn_output_earlier: &TransactionOutput,
    txn_output_later: &TransactionOutput,
    read_set_later: &HashSet<AccessPath>,
) -> bool {
    let earlier_write_set = txn_output_earlier.write_set();
    let later_write_set = txn_output_later.write_set();

    let account_map = get_account_path_map(&earlier_write_set);

    for (write_ap, _) in later_write_set.iter() {
        let write_intersection = find_intersection(write_ap, &account_map);
        if write_intersection {
            return true;
        }
    }

    for read_ap in read_set_later.iter() {
        let read_intersection = find_intersection(read_ap, &account_map);
        if read_intersection {
            return true;
        }
    }
    false
}

pub fn execute_block_sequentially<'alloc>(
    signature_verified_block: Vec<Result<SignatureCheckedTransaction, VMStatus>>,
    mut data_cache: BlockDataCache,
    module_cache: BlockModuleCache<'alloc, '_, ModuleFetcherImpl>,
    script_cache: &ScriptCache<'alloc>,
    mode: ValidationMode,
    publishing_option: &VMPublishingOption,
) -> Vec<TransactionOutput> {

    let mut result = vec![];

    for transaction in signature_verified_block {
        let output = match transaction {
            Ok(t) => transaction_flow(
                t,
                &module_cache,
                script_cache,
                &data_cache,
                mode,
                publishing_option,
            ).1,
            Err(vm_status) => ExecutedTransaction::discard_error_output(vm_status),
        };
        report_execution_status(output.status());
        data_cache.push_write_set(&output.write_set());
        result.push(output);
    }
    trace!("[VM] Execute block finished");
    result
}

pub fn execute_block_parallel<'alloc>(
    block_size: usize,
    signature_verified_block: Vec<Result<SignatureCheckedTransaction, VMStatus>>,
    mut data_cache: BlockDataCache,
    module_cache: BlockModuleCache<'alloc, '_, ModuleFetcherImpl>,
    script_cache: &ScriptCache<'alloc>,
    mode: ValidationMode,
    publishing_option: &VMPublishingOption,
) -> Vec<TransactionOutput> {
    let mut result = vec![
        ExecutedTransaction::discard_error_output(VMStatus::Validation(
            VMValidationStatus::InvalidSignature
        ));
        block_size
    ];

    let mut signature_check_passed: Vec<(usize, SignatureCheckedTransaction)> = vec![];

    for (i, txn) in signature_verified_block.into_iter().enumerate() {
        match txn {
            Ok(t) => signature_check_passed.push((i, t)),
            Err(vm_status) => {
                let transaction_status = ExecutedTransaction::discard_error_output(vm_status);
                report_execution_status(transaction_status.status());
            }
        }
    }

    // The DAG represents the boundaries of the transaction and its indices.
    let mut dag = process_dag(signature_check_passed);

    let mut txn_to_be_executed: VecDeque<(usize, SignatureCheckedTransaction)> = VecDeque::new();

    if dag.num_levels() >= 1 {
        txn_to_be_executed.append(dag.get_level(0));
    }

    let mut count = 1;
    let mut boundary_execution_unfinished = true;

    //until all the nodes have been run
    while !txn_to_be_executed.is_empty() {
        //run the nodes in a boundary in parallel
        print!(
            "Boundary {} has {} transactions\n",
            count,
            txn_to_be_executed.len()
        );
        let mut parallel_boundary_results = txn_to_be_executed
            .par_iter()
            .map(|(_, txn)| {
                transaction_flow(
                    txn.clone(),
                    &module_cache,
                    script_cache,
                    &data_cache,
                    mode,
                    publishing_option,
                )
            })
            .collect::<VecDeque<_>>();

        assert_eq!(parallel_boundary_results.len(), txn_to_be_executed.len());

        //always execute and apply the first transaction
        if parallel_boundary_results.len() >= 1 {
            let (ind, _) = txn_to_be_executed.pop_front().unwrap();
            let (_, output_first) = parallel_boundary_results.pop_front().unwrap();
            report_execution_status(output_first.status());
            data_cache.push_write_set(&output_first.write_set());
            result[ind] = output_first.clone();

            let mut txn_output_earlier = output_first;

            //until the boundary execution has been finished do the following:
            //1. Check for dependencies with previous transaction
            //2. If the consecutive transactions are dependent: end boundary execution here
            //3. If not, apply the current transaction and procede
            while boundary_execution_unfinished {
                match txn_to_be_executed.pop_front() {
                    Some((index, transaction)) => {
                        let (rs, txn_output_later) = parallel_boundary_results.pop_front().unwrap();
                        if determine_dependency(&txn_output_earlier, &txn_output_later, &rs) {
                            boundary_execution_unfinished = false;
                            txn_to_be_executed.push_front((index, transaction));
                        } else {
                            txn_output_earlier = txn_output_later.clone();
                            report_execution_status(txn_output_later.status());
                            data_cache.push_write_set(&txn_output_later.write_set());
                            result[index] = txn_output_later;
                        }
                    }
                    None => {
                        boundary_execution_unfinished = false;
                    }
                }
            }
        }
        // if there are any more boundaries, add them here
        if count < dag.num_levels() {
            txn_to_be_executed.append(dag.get_level(count));
        }
        count += 1;
    }
    trace!("[VM] Execute block finished");
    result

}

pub fn execute_block<'alloc>(
    txn_block: Vec<SignedTransaction>,
    code_cache: &VMModuleCache<'alloc>,
    script_cache: &ScriptCache<'alloc>,
    data_view: &dyn StateView,
    publishing_option: &VMPublishingOption,
    execution_style: Execute,
) -> Vec<TransactionOutput> {
    trace!("[VM] Execute block, transaction count: {}", txn_block.len());
    report_block_count(txn_block.len());

    let block_size = txn_block.len();

    let mode = if data_view.is_genesis() {
        // The genesis transaction must be in a block of its own.
        if txn_block.len() != 1 {
            // XXX Need a way to return that an entire block failed.
            return txn_block
                .iter()
                .map(|_| {
                    TransactionOutput::new(
                        WriteSet::default(),
                        vec![],
                        0,
                        TransactionStatus::from(VMStatus::Validation(
                            VMValidationStatus::RejectedWriteSet,
                        )),
                    )
                })
                .collect();
        } else {
            ValidationMode::Genesis
        }
    } else {
        ValidationMode::Executing
    };

    let module_cache = BlockModuleCache::new(code_cache, ModuleFetcherImpl::new(data_view));
    let data_cache = BlockDataCache::new(data_view);


    //Perform Signature Checking in parallel
    let signature_verified_block: Vec<Result<SignatureCheckedTransaction, VMStatus>> = txn_block
        .into_par_iter()
        .map(|txn| match txn.check_signature() {
            Ok(t) => Ok(t),
            Err(_) => Err(VMStatus::Validation(VMValidationStatus::InvalidSignature)),
        })
        .collect();

    match execution_style {
        Execute::Parallel => execute_block_parallel(block_size, signature_verified_block, data_cache, module_cache, script_cache, mode, publishing_option),
        Execute::Sequential => execute_block_sequentially(signature_verified_block, data_cache, module_cache, script_cache, mode, publishing_option),
    }

}

/// Process a transaction and emit a TransactionOutput.
///
/// A successful execution will have `TransactionStatus::Keep` in the TransactionOutput and a
/// non-empty writeset. There are two possibilities for a failed transaction. If a verification or
/// runtime error occurs, the TransactionOutput will have `TransactionStatus::Keep` and a writeset
/// that only contains the charged gas of this transaction. If a validation or `InvariantViolation`
/// error occurs, the TransactionOutput will have `TransactionStatus::Discard` and an empty
/// writeset.
///
/// Note that this function DO HAVE side effect. If a transaction tries to publish some module,
/// and this transaction is executed successfully, this function will update `module_cache` to
/// include those newly published modules. This function will also update the `script_cache` to
/// cache this `txn`
fn transaction_flow<'alloc, P>(
    txn: SignatureCheckedTransaction,
    module_cache: P,
    script_cache: &ScriptCache<'alloc>,
    data_cache: &BlockDataCache<'_>,
    mode: ValidationMode,
    publishing_option: &VMPublishingOption,
) -> (HashSet<AccessPath>, TransactionOutput)
where
    P: ModuleCache<'alloc>,
{
    let arena = Arena::new();
    let process_txn = ProcessTransaction::new(txn, &module_cache, data_cache, &arena);

    let validated_txn = match process_txn.validate(mode, publishing_option) {
        Ok(validated_txn) => validated_txn,
        Err(vm_status) => {
            return (
                HashSet::new(),
                ExecutedTransaction::discard_error_output(vm_status),
            );
        }
    };
    let verified_txn = match validated_txn.verify(script_cache) {
        Ok(verified_txn) => verified_txn,
        Err(vm_status) => {
            return (
                HashSet::new(),
                ExecutedTransaction::discard_error_output(vm_status),
            );
        }
    };
    let mut executed_txn = verified_txn.execute();

    let read_set = executed_txn.read_set();

    // On success, publish the modules into the cache so that future transactions can refer to them
    // directly.
    let output = executed_txn.into_output();
    if let TransactionStatus::Keep(VMStatus::Execution(ExecutionStatus::Executed)) = output.status()
    {
        module_cache.reclaim_cached_module(arena.into_vec());
    };
    (read_set, output)
}
