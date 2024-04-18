package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

var HEADERS = []string{
	"Timestamp",
	"Operation type",
	"Operation sub type",
	"Base amount",
	"Base currency",
	"Fiat amount",
	"Quote amount",
	"Quote currency",
	"Fee amount",
	"Fee currency",
	"Fee Fiat Amount",
	"From",
	"To",
	"Transaction Id",
	"Notes",
}

type TransformBinance struct {
	OutputFilename string
	InputFile      string
}

func NewTransformBinance(outputFilename, inputFile string) *TransformBinance {
	return &TransformBinance{
		OutputFilename: outputFilename,
		InputFile:      inputFile,
	}
}

func (t *TransformBinance) Transform() error {
	file, err := os.Open(t.InputFile)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	writer, err := os.Create(t.OutputFilename)
	if err != nil {
		return err
	}
	defer writer.Close()

	csvWriter := csv.NewWriter(writer)
	defer csvWriter.Flush()

	err = csvWriter.Write(HEADERS)
	if err != nil {
		return err
	}

	dataArray := make([][]string, 0)

	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if line[0] == "User_ID" {
			continue
		}

		timeStr := line[1]
		typeStr := strings.Trim(line[3], "\"")
		asset := strings.Trim(line[4], "\"")
		amountStr := strings.Trim(line[5], "\"")
		note := typeStr + "--" + strings.Trim(line[6], "\"")

		time, err := time.Parse("2006-01-02 15:04:05", timeStr)
		if err != nil {
			return err
		}
		timeUnix := time.Unix()

		amount, err := strconv.ParseFloat(amountStr, 64)
		if err != nil {
			return err
		}

		var data []string

		if typeStr == "Deposit" || typeStr == "Fiat Deposit" || typeStr == "Binance Card Cashback" || typeStr == "Binance Card Spending" || typeStr == "Sell to Card" ||
			typeStr == "Buy Crypto" || typeStr == "Insurance Fund Compensation" || typeStr == "Card Cashback" || typeStr == "send" || typeStr == "Send" || typeStr == "C2C Transfer" || typeStr == "Sell Crypto To Fiat" {
			data = []string{strconv.FormatInt(timeUnix-2, 10), "deposit", "", strconv.FormatFloat(amount, 'f', -1, 64), asset, note}
		} else if typeStr == "Withdraw" || typeStr == "send" || typeStr == "Send" || typeStr == "C2C Transfer" || typeStr == "Fiat Withdrawal" || typeStr == "Fiat Withdraw" {
			data = []string{strconv.FormatInt(timeUnix+2, 10), "withdrawal", "", strconv.FormatFloat(amount, 'f', -1, 64), asset, note}
		} else if typeStr == "BNB Vault Rewards" || typeStr == "Realized Profit and Loss" || typeStr == "Asset Recovery" || typeStr == "Realize profit and loss" || typeStr == "Launchpool Earnings Withdrawal" {
			data = []string{strconv.FormatInt(timeUnix, 10), "", "realized_pnl", strconv.FormatFloat(amount, 'f', -1, 64), asset, note}
		} else if typeStr == "Buy" || typeStr == "Transaction Related" || typeStr == "Large OTC Trading" || typeStr == "Buy Crypto" || typeStr == "Sell" || typeStr == "Binance Convert" ||
			typeStr == "Transaction Buy" || typeStr == "Transaction Spend" || typeStr == "Transaction Sold" || typeStr == "Transaction Revenue" || typeStr == "Stablecoins Auto-Conversion" {
			data = []string{strconv.FormatInt(timeUnix, 10), "", "swap", strconv.FormatFloat(amount, 'f', -1, 64), asset, note}
		} else if typeStr == "Small Assets Exchange BNB" || typeStr == "Small assets exchange BNB" {
			data = []string{strconv.FormatInt(timeUnix, 10), "", "swap", strconv.FormatFloat(amount, 'f', -1, 64), asset, strings.Trim(line[6], "\"")}
		} else if typeStr == "Commission Fee Shared With You" || typeStr == "Commission Rebate" {
			data = []string{strconv.FormatInt(timeUnix, 10), "", "commission_rebate", strconv.FormatFloat(amount, 'f', -1, 64), asset, note}
		} else if typeStr == "Distribution" || typeStr == "Mission Reward Distribution" || typeStr == "Token Swap - Distribution" || typeStr == "Airdrop Assets" {
			data = []string{strconv.FormatInt(timeUnix, 10), "", "airdrop", strconv.FormatFloat(amount, 'f', -1, 64), asset, note}
		} else if typeStr == "Fee" || typeStr == "Transaction Fee" || typeStr == "Funding Fee" {
			data = []string{strconv.FormatInt(timeUnix, 10), "", "commission", strconv.FormatFloat(amount, 'f', -1, 64), asset, note}
		} else if typeStr == "Referral Kickback" {
			data = []string{strconv.FormatInt(timeUnix, 10), "", "referral_kickback", strconv.FormatFloat(amount, 'f', -1, 64), asset, note}
		} else if typeStr == "Simple Earn Flexible Subscription" || typeStr == "Simple Earn Locked Subscription" || typeStr == "Launchpool Subscription/Redemption" ||
			typeStr == "Simple Earn Flexible Redemption" || typeStr == "Simple Earn Locked Redemption" || typeStr == "Leverage token redemption" ||
			typeStr == "Staking Purchase" || typeStr == "Staking Redemption" || typeStr == "Staking Unlocked" || typeStr == "ETH 2.0 Staking" || typeStr == "IsolatedMargin loan" || typeStr == "IsolatedMargin repayment" {
			data = []string{strconv.FormatInt(timeUnix, 10), "", "lending", strconv.FormatFloat(amount, 'f', -1, 64), asset, note}
		} else if typeStr == "Savings Distribution" || typeStr == "Simple Earn Flexible Interest" || typeStr == "Staking Rewards" || typeStr == "Simple Earn Locked Rewards" || typeStr == "ETH 2.0 Staking Rewards" {
			data = []string{strconv.FormatInt(timeUnix, 10), "", "staking_reward", strconv.FormatFloat(amount, 'f', -1, 64), asset, note}
		} else if typeStr == "Transfer Between Main and Funding Wallet" || typeStr == "Transfer from Main Account/Futures to Margin Account" ||
			typeStr == "Transfer from Margin Account to Main Account/Futures" || typeStr == "Transfer Between Spot Account and UM Futures Account" ||
			typeStr == "Futures Account Transfer" || typeStr == "Transfer Between Sub-Account UM Futures and Spot Account" || typeStr == "Sub-account Transfer" ||
			typeStr == "Asset Conversion Transfer" || typeStr == "Transfer Between Main Account/Futures and Margin Account" || typeStr == "Main and Funding Account Transfer" {
			data = []string{strconv.FormatInt(timeUnix, 10), "", "internal_transfer", strconv.FormatFloat(amount, 'f', -1, 64), asset, note}
		} else if typeStr == "Auto-Invest Transaction" {
			data = []string{strconv.FormatInt(timeUnix, 10), "", "swap", strconv.FormatFloat(amount, 'f', -1, 64), asset, note}
		} else {
			return fmt.Errorf("Unknown type: %s", typeStr)
		}

		dataArray = append(dataArray, data)
	}

	hash := make(map[int64][][]string)
	for _, a := range dataArray {
		time := a[0]
		timeInt, err := strconv.ParseInt(time, 10, 64)
		if err != nil {
			return err
		}
		hash[timeInt/10] = append(hash[timeInt/10], a)
	}

	for _, array := range hash {
		for _, a := range array {
			typeStr := "deposit"
			amount, err := strconv.ParseFloat(a[3], 64)
			if err == nil {
				if amount < 0 {
					typeStr = "withdrawal"
				}
			}
			csvWriter.Write([]string{a[0], typeStr, a[2], strconv.FormatFloat(math.Abs(amount), 'f', -1, 64), a[4], "", "", "", "", "", "", "", "", "", a[5]})
		}
	}

	return nil
}

func main() {

	inputFile := flag.String("i", "", "input file")
	outputFile := flag.String("o", "", "output file")
	flag.Parse()

	if *inputFile == "" || *outputFile == "" {
		log.Fatal("Usage: go run main.go -input <input_file> -output <output_file>")
	}

	transformer := NewTransformBinance(*outputFile, *inputFile)
	err := transformer.Transform()
	if err != nil {
		log.Fatal(err)
	}
}
