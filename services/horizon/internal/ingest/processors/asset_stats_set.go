package processors

import (
	"math/big"

	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

type assetStatKey struct {
	assetType   xdr.AssetType
	assetCode   string
	assetIssuer string
}

type assetStatValue struct {
	assetStatKey
	balances assetStatBalances
	accounts history.ExpAssetStatAccounts
}

type assetStatBalances struct {
	Authorized                      *big.Int
	AuthorizedToMaintainLiabilities *big.Int
	ClaimableBalances               *big.Int
	Unauthorized                    *big.Int
}

func (a *assetStatBalances) Parse(b *history.ExpAssetStatBalances) error {
	authorized, ok := new(big.Int).SetString(b.Authorized, 10)
	if !ok {
		return errors.New("Error parsing: " + b.Authorized)
	}
	a.Authorized = authorized

	authorizedToMaintainLiabilities, ok := new(big.Int).SetString(b.AuthorizedToMaintainLiabilities, 10)
	if !ok {
		return errors.New("Error parsing: " + b.AuthorizedToMaintainLiabilities)
	}
	a.AuthorizedToMaintainLiabilities = authorizedToMaintainLiabilities

	claimableBalances, ok := new(big.Int).SetString(b.ClaimableBalances, 10)
	if !ok {
		return errors.New("Error parsing: " + b.ClaimableBalances)
	}
	a.ClaimableBalances = claimableBalances

	unauthorized, ok := new(big.Int).SetString(b.Unauthorized, 10)
	if !ok {
		return errors.New("Error parsing: " + b.Unauthorized)
	}
	a.Unauthorized = unauthorized

	return nil
}

func (a assetStatBalances) Add(b assetStatBalances) assetStatBalances {
	return assetStatBalances{
		Authorized:                      big.NewInt(0).Add(a.Authorized, b.Authorized),
		AuthorizedToMaintainLiabilities: big.NewInt(0).Add(a.AuthorizedToMaintainLiabilities, b.AuthorizedToMaintainLiabilities),
		ClaimableBalances:               big.NewInt(0).Add(a.ClaimableBalances, b.ClaimableBalances),
		Unauthorized:                    big.NewInt(0).Add(a.Unauthorized, b.Unauthorized),
	}
}

func (a assetStatBalances) IsZero() bool {
	return a.Authorized.Cmp(big.NewInt(0)) == 0 &&
		a.AuthorizedToMaintainLiabilities.Cmp(big.NewInt(0)) == 0 &&
		a.ClaimableBalances.Cmp(big.NewInt(0)) == 0 &&
		a.Unauthorized.Cmp(big.NewInt(0)) == 0
}

func (a assetStatBalances) ConvertToHistoryObject() history.ExpAssetStatBalances {
	return history.ExpAssetStatBalances{
		Authorized:                      a.Authorized.String(),
		AuthorizedToMaintainLiabilities: a.AuthorizedToMaintainLiabilities.String(),
		ClaimableBalances:               a.ClaimableBalances.String(),
		Unauthorized:                    a.Unauthorized.String(),
	}
}

func (value assetStatValue) ConvertToHistoryObject() history.ExpAssetStat {
	balances := value.balances.ConvertToHistoryObject()
	return history.ExpAssetStat{
		AssetType:   value.assetType,
		AssetCode:   value.assetCode,
		AssetIssuer: value.assetIssuer,
		Accounts:    value.accounts,
		Balances:    balances,
		Amount:      balances.Authorized,
		NumAccounts: value.accounts.Authorized,
	}
}

// AssetStatSet represents a collection of asset stats
type AssetStatSet map[assetStatKey]*assetStatValue

// Add updates the set with a trustline entry from a history archive snapshot.
func (s AssetStatSet) Add(trustLine xdr.TrustLineEntry) error {
	var deltaBalances deltas
	var deltaAccounts deltas
	deltaBalances.AddByFlags(trustLine.Flags, int64(trustLine.Balance))
	deltaAccounts.AddByFlags(trustLine.Flags, 1)

	return s.AddDelta(trustLine.Asset, deltaBalances, deltaAccounts)
}

type deltas struct {
	Authorized                      int64
	AuthorizedToMaintainLiabilities int64
	Unauthorized                    int64
	ClaimableBalances               int64
}

func (d *deltas) AddByFlags(flags xdr.Uint32, amount int64) {
	switch xdr.TrustLineFlags(flags) {
	case xdr.TrustLineFlagsAuthorizedFlag:
		d.Authorized += amount
	case xdr.TrustLineFlagsAuthorizedToMaintainLiabilitiesFlag:
		d.AuthorizedToMaintainLiabilities += amount
	default:
		d.Unauthorized += amount
	}
}

func (d deltas) isEmpty() bool {
	return d == deltas{}
}

// AddDelta adds a delta balance and delta accounts to a given asset trustline.
func (s AssetStatSet) AddDelta(asset xdr.Asset, deltaBalances, deltaAccounts deltas) error {
	if deltaBalances.isEmpty() && deltaAccounts.isEmpty() {
		return nil
	}

	var key assetStatKey
	if err := asset.Extract(&key.assetType, &key.assetCode, &key.assetIssuer); err != nil {
		return errors.Wrap(err, "could not extract asset info from trustline")
	}

	current, ok := s[key]
	if !ok {
		current = &assetStatValue{assetStatKey: key, balances: assetStatBalances{
			Authorized:                      big.NewInt(0),
			AuthorizedToMaintainLiabilities: big.NewInt(0),
			ClaimableBalances:               big.NewInt(0),
			Unauthorized:                    big.NewInt(0),
		}}
		s[key] = current
	}

	current.accounts.Authorized += int32(deltaAccounts.Authorized)
	current.accounts.AuthorizedToMaintainLiabilities += int32(deltaAccounts.AuthorizedToMaintainLiabilities)
	current.accounts.ClaimableBalances += int32(deltaAccounts.ClaimableBalances)
	current.accounts.Unauthorized += int32(deltaAccounts.Unauthorized)

	current.balances.Authorized.Add(current.balances.Authorized, big.NewInt(deltaBalances.Authorized))
	current.balances.AuthorizedToMaintainLiabilities.Add(current.balances.AuthorizedToMaintainLiabilities, big.NewInt(deltaBalances.AuthorizedToMaintainLiabilities))
	current.balances.ClaimableBalances.Add(current.balances.ClaimableBalances, big.NewInt(deltaBalances.ClaimableBalances))
	current.balances.Unauthorized.Add(current.balances.Unauthorized, big.NewInt(deltaBalances.Unauthorized))

	// Note: it's possible that after operations above:
	// numAccounts != 0 && amount == 0 (ex. two accounts send some of their assets to third account)
	//  OR
	// numAccounts == 0 && amount != 0 (ex. issuer issued an asset)
	if current.balances.IsZero() && current.accounts.IsZero() {
		delete(s, key)
	}

	return nil
}

// Remove deletes an asset stat from the set
func (s AssetStatSet) Remove(assetType xdr.AssetType, assetCode string, assetIssuer string) (history.ExpAssetStat, bool) {
	key := assetStatKey{assetType: assetType, assetIssuer: assetIssuer, assetCode: assetCode}
	value, ok := s[key]
	if !ok {
		return history.ExpAssetStat{}, false
	}

	delete(s, key)

	return value.ConvertToHistoryObject(), true
}

// All returns a list of all `history.ExpAssetStat` contained within the set
func (s AssetStatSet) All() []history.ExpAssetStat {
	assetStats := make([]history.ExpAssetStat, 0, len(s))
	for _, value := range s {
		assetStats = append(assetStats, value.ConvertToHistoryObject())
	}
	return assetStats
}
