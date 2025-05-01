package core

type BillingMode uint8

const (
	BILLING_MODE_PROVISIONED BillingMode = iota
	BILLING_MODE_PAY_PER_REQUEST
)
