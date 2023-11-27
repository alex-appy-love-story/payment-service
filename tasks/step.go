package tasks

import (
	"fmt"
	"log"

	"github.com/alex-appy-love-story/db-lib/models/order"
	"github.com/alex-appy-love-story/db-lib/models/token"
	"github.com/alex-appy-love-story/db-lib/models/user"
	"github.com/shopspring/decimal"
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

func Perform(p StepPayload, ctx TaskContext) (err error) {
	if p.Action == "err" {
		return fmt.Errorf("Test error!!!")
	}

	log.Printf("%+v\n", p)

	err = ctx.GormClient.Transaction(func(tsx *gorm.DB) error {

		tok, err := token.GetToken(tsx, p.TokenID)
		if err != nil {
			return err
		}

		// Retrieve the total cost of the order.
		totalCost := tok.Cost.Mul(decimal.NewFromInt32(int32(p.Amount)))

		log.Println("perform: find user")

		// Retrieve user balance.
		usr, err := user.GetUserByUsername(tsx, p.Username)
		if err != nil {

			// Create new user if DNE.
			if err.Error() == "record not found" {
				usr, err = user.CreateUser(tsx, p.Username)
				if err != nil {
					return err
				}

			} else {
				return err
			}
		}

		// User can't afford.
		if !usr.Balance.GreaterThanOrEqual(totalCost) {

			err := SetOrderStatus(ctx.OrderSvcAddr, p.OrderID, order.PAYMENT_FAIL_INSUFFICIENT)
			if err != nil {
				return fmt.Errorf("failed to set status")
			}

			return fmt.Errorf("insufficient funds")
		}

		_, err = user.UpdateUserBalance(tsx, usr.ID, usr.Balance.Sub(totalCost))
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		fmt.Println("Reverting previous")
		RevertPrevious(p, map[string]interface{}{"order_id": p.OrderID}, ctx)
		return err
	}

	nextPayload := map[string]interface{}{
		"amount":   p.Amount,
		"token_id": p.TokenID,
	}

	return PerformNext(p, nextPayload, ctx)
}

func Revert(p StepPayload, ctx TaskContext) error {
	err := ctx.GormClient.Transaction(func(tsx *gorm.DB) error {

		tok, err := token.GetToken(tsx, p.TokenID)
		if err != nil {
			return err
		}

		// Retrieve the total cost of the order.
		totalCost := tok.Cost.Mul(decimal.NewFromInt32(int32(p.Amount)))

		// Retrieve user balance.
		usr, err := user.GetUserByUsername(tsx, p.Username)
		if err != nil {
			return err
		}

		_, err = user.UpdateUserBalance(tsx, usr.ID, usr.Balance.Add(totalCost))
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}
	previousPayload := map[string]interface{}{
		"order_id": p.OrderID,
	}

	return RevertPrevious(p, previousPayload, ctx)
}
