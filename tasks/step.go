package tasks

import (
	"fmt"

	"github.com/alex-appy-love-story/db-lib/models/order"
	"github.com/alex-appy-love-story/db-lib/models/token"
	"github.com/alex-appy-love-story/db-lib/models/user"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
)

//----------------------------------------------
// Task payload.
//---------------------------------------------

type StepPayload struct {
	SagaPayload

	// Define members here...
	order.Order

	OrderID  uint   `json:"order_id"`
	Username string `json:"username"`
}

func Perform(p StepPayload, ctx *TaskContext) (err error) {
	ctx.Span.AddEvent("Making payment")

	err = ctx.GormClient.Transaction(func(tsx *gorm.DB) error {

		tok, err := token.GetToken(tsx, p.TokenID)
		if err != nil {
			return err
		}

		// Retrieve the total cost of the order.
		totalCost := tok.Cost.Mul(decimal.NewFromInt32(int32(p.Amount)))

		ctx.Span.AddEvent("Fetching user information")

		// Retrieve user balance.
		usr, err := user.GetUserByUsername(tsx, p.Username)
		if err != nil {

			// Create new user if DNE.
			if err.Error() == "record not found" {
				ctx.Span.AddEvent(fmt.Sprintf("Creating a new user: %s", p.Username))
				usr, err = user.CreateUser(tsx, p.Username)
				if err != nil {
					return fmt.Errorf("Failed to create user: %s, reason: %s", p.Username, err.Error())
				}

			} else {
				return err
			}
		}

		ctx.Span.AddEvent("Checking user balance")
		// User can't afford.
		if !usr.Balance.GreaterThanOrEqual(totalCost) {
			err := SetOrderStatus(ctx.OrderSvcAddr, p.OrderID, order.PAYMENT_FAIL_INSUFFICIENT)
			if err != nil {
				return fmt.Errorf("Failed to set order status")
			}
			return fmt.Errorf("User has insufficient funds. cost: %d, balance: %d", totalCost, usr.Balance)
		}

		ctx.Span.AddEvent("User has sufficient funds, deducting")
		_, err = user.UpdateUserBalance(tsx, usr.ID, usr.Balance.Sub(totalCost))
		if err != nil {
			return fmt.Errorf("Failed to update user balance")
		}

		return nil
	})

	if err != nil {
		ctx.Span.AddEvent("Transaction error, rolling back")
		RevertPrevious(p, map[string]interface{}{"order_id": p.OrderID}, ctx)
		return err
	}

	nextPayload := map[string]interface{}{
		"amount":   p.Amount,
		"token_id": p.TokenID,
		"username": p.Username,
		"order_id": p.OrderID,
	}

	ctx.Span.AddEvent("Successfully processed payment")

	return PerformNext(p, nextPayload, ctx)
}

func Revert(p StepPayload, ctx *TaskContext) error {
	ctx.Span.AddEvent("Refunding payment")

	err := ctx.GormClient.Transaction(func(tsx *gorm.DB) error {

		ctx.Span.AddEvent("Retrieving token info", trace.WithAttributes(attribute.Int("token_id", int(p.TokenID))))
		tok, err := token.GetToken(tsx, p.TokenID)
		if err != nil {
			return fmt.Errorf("Failed to retrieve token info, ID: %d", p.TokenID)
		}

		// Retrieve the total cost of the order.
		totalCost := tok.Cost.Mul(decimal.NewFromInt32(int32(p.Amount)))

		ctx.Span.AddEvent("Retrieving user balance", trace.WithAttributes(attribute.Int("user_id", int(p.UserID))))
		// Retrieve user balance.
		usr, err := user.GetUserByUsername(tsx, p.Username)
		if err != nil {
			return fmt.Errorf("Failed to retrieve user balance")
		}

		ctx.Span.AddEvent("Refunding user", trace.WithAttributes(attribute.Int("user_id", int(p.UserID))))
		_, err = user.UpdateUserBalance(tsx, usr.ID, usr.Balance.Add(totalCost))
		if err != nil {
			return fmt.Errorf("Failed to update user balance")
		}

		return nil
	})
	if err != nil {
		return err
	}
	previousPayload := map[string]interface{}{
		"order_id": p.OrderID,
	}

	ctx.Span.AddEvent("Successfully refunded")

	return RevertPrevious(p, previousPayload, ctx)
}
