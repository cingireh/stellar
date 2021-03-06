package resourceadapter

import (
	"context"
	"strings"

	"github.com/stellar/go/amount"
	protocol "github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/render/hal"
	"github.com/stellar/go/xdr"
)

// PopulateAssetStat populates an AssetStat using asset stats and account entries
// generated from the ingestion system.
func PopulateAssetStat(
	ctx context.Context,
	res *protocol.AssetStat,
	row history.ExpAssetStat,
	issuer history.AccountEntry,
) (err error) {
	res.Asset.Type = xdr.AssetTypeToString[row.AssetType]
	res.Asset.Code = row.AssetCode
	res.Asset.Issuer = row.AssetIssuer
	res.Accounts = protocol.AssetStatAccounts(row.Accounts)
	res.Amount, err = amount.IntStringToAmount(row.Amount)
	if err != nil {
		return errors.Wrap(err, "Invalid amount in PopulateAssetStat")
	}
	err = populateAssetStatBalances(&res.Balances, row.Balances)
	if err != nil {
		return err
	}
	res.NumAccounts = row.NumAccounts
	flags := int8(issuer.Flags)
	res.Flags = protocol.AccountFlags{
		(flags & int8(xdr.AccountFlagsAuthRequiredFlag)) != 0,
		(flags & int8(xdr.AccountFlagsAuthRevocableFlag)) != 0,
		(flags & int8(xdr.AccountFlagsAuthImmutableFlag)) != 0,
		(flags & int8(xdr.AccountFlagsAuthClawbackEnabledFlag)) != 0,
	}
	res.PT = row.PagingToken()

	trimmed := strings.TrimSpace(issuer.HomeDomain)
	var toml string
	if trimmed != "" {
		toml = "https://" + issuer.HomeDomain + "/.well-known/stellar.toml"
	}
	res.Links.Toml = hal.NewLink(toml)
	return
}

func populateAssetStatBalances(res *protocol.AssetStatBalances, row history.ExpAssetStatBalances) (err error) {
	res.Authorized, err = amount.IntStringToAmount(row.Authorized)
	if err != nil {
		return errors.Wrapf(err, "Invalid amount in PopulateAssetStatBalances: %q", row.Authorized)
	}

	res.AuthorizedToMaintainLiabilities, err = amount.IntStringToAmount(row.AuthorizedToMaintainLiabilities)
	if err != nil {
		return errors.Wrapf(err, "Invalid amount in PopulateAssetStatBalances: %q", row.AuthorizedToMaintainLiabilities)
	}

	res.Unauthorized, err = amount.IntStringToAmount(row.Unauthorized)
	if err != nil {
		return errors.Wrapf(err, "Invalid amount in PopulateAssetStatBalances: %q", row.Unauthorized)
	}

	return nil
}
