package history

import (
	"testing"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/guregu/null"
	"github.com/stellar/go/services/horizon/internal/test"
	"github.com/stellar/go/services/horizon/internal/toid"
)

func assertCountRows(tt *test.T, q *Q, tables []string, expectedCount int) {
	for _, table := range tables {
		sql := sq.Select("count(*)").From(table)
		var count int
		err := q.Get(&count, sql)
		tt.Assert.NoError(err)
		tt.Assert.Equal(expectedCount, count)
	}
}

func TestRemoveExpIngestHistory(t *testing.T) {
	tt := test.Start(t)
	defer tt.Finish()
	test.ResetHorizonDB(t, tt.HorizonDB)
	q := &Q{tt.HorizonSession()}

	summary, err := q.RemoveExpIngestHistory(69859)
	tt.Assert.Equal(ExpIngestRemovalSummary{}, summary)
	tt.Assert.NoError(err)

	txInsertBuilder := q.NewTransactionBatchInsertBuilder(0)
	txParticipantsInsertBuilder := q.NewTransactionParticipantsBatchInsertBuilder(0)
	opInsertBuilder := q.NewOperationBatchInsertBuilder(0)
	opParticipantsInsertBuilder := q.NewOperationParticipantBatchInsertBuilder(0)
	accountID := int64(1223)

	expTables := []string{
		"exp_history_ledgers",
		"exp_history_transactions",
		"exp_history_transaction_participants",
		"exp_history_operations",
		"exp_history_operation_participants",
	}

	ledger := Ledger{
		Sequence:                   69859,
		PreviousLedgerHash:         null.NewString("4b0b8bace3b2438b2404776ce57643966855487ba6384724a3c664c7aa4cd9e4", true),
		ImporterVersion:            321,
		TransactionCount:           12,
		SuccessfulTransactionCount: new(int32),
		FailedTransactionCount:     new(int32),
		OperationCount:             23,
		TotalCoins:                 23451,
		FeePool:                    213,
		BaseReserve:                687,
		MaxTxSetSize:               345,
		ProtocolVersion:            12,
		BaseFee:                    100,
		ClosedAt:                   time.Now().UTC().Truncate(time.Second),
		LedgerHeaderXDR:            null.NewString("temp", true),
	}
	*ledger.SuccessfulTransactionCount = 12
	*ledger.FailedTransactionCount = 3
	hashes := []string{
		"4db1e4f145e9ee75162040d26284795e0697e2e84084624e7c6c723ebbf80118",
		"5db1e4f145e9ee75162040d26284795e0697e2e84084624e7c6c723ebbf80118",
		"6db1e4f145e9ee75162040d26284795e0697e2e84084624e7c6c723ebbf80118",
		"7db1e4f145e9ee75162040d26284795e0697e2e84084624e7c6c723ebbf80118",
		"8db1e4f145e9ee75162040d26284795e0697e2e84084624e7c6c723ebbf80118",
	}

	for i, hash := range hashes {
		ledger.TotalOrderID.ID = toid.New(ledger.Sequence, 0, 0).ToInt64()
		ledger.LedgerHash = hash
		if i > 0 {
			ledger.PreviousLedgerHash = null.NewString(hashes[i-1], true)
		}

		insertSQL := sq.Insert("exp_history_ledgers").SetMap(ledgerToMap(ledger))
		_, err = q.Exec(insertSQL)
		tt.Assert.NoError(err)

		tx := buildLedgerTransaction(
			tt.T,
			testTransaction{
				index:         1,
				envelopeXDR:   "AAAAACiSTRmpH6bHC6Ekna5e82oiGY5vKDEEUgkq9CB//t+rAAAAyAEXUhsAADDRAAAAAAAAAAAAAAABAAAAAAAAAAsBF1IbAABX4QAAAAAAAAAA",
				resultXDR:     "AAAAAAAAASwAAAAAAAAAAwAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAFAAAAAAAAAAA=",
				feeChangesXDR: "AAAAAA==",
				metaXDR:       "AAAAAQAAAAAAAAAA",
				hash:          "19aaa18db88605aedec04659fb45e06f240b022eb2d429e05133e4d53cd945ba",
			},
		)

		err = txInsertBuilder.Add(
			tx,
			uint32(ledger.Sequence),
		)
		tt.Assert.NoError(err)
		tt.Assert.NoError(txInsertBuilder.Exec())
		tt.Assert.NoError(
			txParticipantsInsertBuilder.Add(toid.New(ledger.Sequence, 1, 0).ToInt64(), accountID),
		)
		tt.Assert.NoError(txParticipantsInsertBuilder.Exec())

		err = opInsertBuilder.Add(
			tx,
			uint32(ledger.Sequence),
		)
		tt.Assert.NoError(err)
		tt.Assert.NoError(opInsertBuilder.Exec())
		tt.Assert.NoError(
			opParticipantsInsertBuilder.Add(toid.New(ledger.Sequence, 1, 1).ToInt64(), accountID),
		)
		tt.Assert.NoError(opParticipantsInsertBuilder.Exec())

		ledger.Sequence++
	}

	assertCountRows(tt, q, expTables, 5)

	summary, err = q.RemoveExpIngestHistory(69863)
	tt.Assert.Equal(ExpIngestRemovalSummary{}, summary)
	tt.Assert.NoError(err)

	assertCountRows(tt, q, expTables, 5)

	cutoffSequence := 69861
	summary, err = q.RemoveExpIngestHistory(uint32(cutoffSequence))
	tt.Assert.Equal(
		ExpIngestRemovalSummary{
			LedgersRemoved:                 2,
			TransactionsRemoved:            2,
			TransactionParticipantsRemoved: 2,
			OperationsRemoved:              2,
			OperationParticipantsRemoved:   2,
		},
		summary,
	)
	tt.Assert.NoError(err)

	var ledgers []Ledger
	err = q.Select(&ledgers, selectLedgerFields.From("exp_history_ledgers hl"))
	tt.Assert.NoError(err)
	tt.Assert.Len(ledgers, 3)

	var transactions []Transaction
	err = q.Select(&transactions, selectExpTransaction)
	tt.Assert.NoError(err)
	tt.Assert.Len(transactions, 3)

	txParticipants := getTransactionParticipants(tt, q)
	tt.Assert.Len(txParticipants, 3)

	var operations []Operation
	err = q.Select(&operations, selectExpOperation)
	tt.Assert.NoError(err)
	tt.Assert.Len(operations, 3)

	type hop struct {
		OperationID int64 `db:"history_operation_id"`
		AccountID   int64 `db:"history_account_id"`
	}
	var opParticipants []hop
	err = q.Select(&opParticipants, sq.Select(
		"hopp.history_operation_id, "+
			"hopp.history_account_id").
		From("exp_history_operation_participants hopp"),
	)
	tt.Assert.NoError(err)
	tt.Assert.Len(opParticipants, 3)

	nextLedger := toid.ID{LedgerSequence: int32(cutoffSequence + 1)}
	for i := range ledgers {
		tt.Assert.LessOrEqual(ledgers[i].Sequence, int32(cutoffSequence))
		tt.Assert.LessOrEqual(transactions[i].LedgerSequence, int32(cutoffSequence))

		tt.Assert.Less(txParticipants[i].TransactionID, nextLedger.ToInt64())
		tt.Assert.Less(operations[i].TransactionID, nextLedger.ToInt64())
		tt.Assert.Less(operations[i].ID, nextLedger.ToInt64())
		tt.Assert.Less(opParticipants[i].OperationID, nextLedger.ToInt64())
	}
}