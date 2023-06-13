package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/TikTokTechImmersion/assignment_demo_2023/rpc-server/kitex_gen/rpc"

	"database/sql"

	"github.com/avast/retry-go"
	_ "github.com/go-sql-driver/mysql"
)

// IMServiceImpl implements the last service interface defined in the IDL.
type IMServiceImpl struct{}

var db *sql.DB

func init() {
	fmt.Println('0')
	db, err := Connection()
	if err != nil {
		log.Println(err.Error())
	}
	CreateTable(db)
}

func Connection() (*sql.DB, error) {
	var db *sql.DB
	var err error

	// Retry connecting to the MySQL server with exponential backoff
	err = retry.Do(
		func() error {
			db, err := sql.Open("mysql", "docker:password@tcp(mysql:3306)/messages")
			if err != nil {
				log.Println("Failed to connect to MySQL server:", err)
				return err
			}

			// Check the connection
			err = db.Ping()
			if err != nil {
				log.Println("Failed to ping MySQL server:", err)
				return err
			}

			return nil
		},
		retry.Delay(time.Second),   // Wait 1 second between retries
		retry.MaxDelay(time.Minute), // Maximum delay between retries: 1 minute
		retry.Attempts(5),           // Maximum number of retry attempts: 5
		retry.OnRetry(func(n uint, err error) {
			log.Printf("Retry attempt %d failed: %s\n", n, err)
		}),
	)

	if err != nil {
		log.Fatal("Failed to connect to MySQL server after retries:", err)
	}

	fmt.Println("Connected to MySQL!")
	return db, nil
}

func CreateTable (*sql.DB) {
	m, err := db.Query("CREATE TABLE 'messages' ('ID'	INTEGER,'Chat'	INTEGER,'Text'	TEXT,'Sender'	INTEGER,'SendTime'	TEXT,PRIMARY KEY('ID' AUTOINCREMENT));")

	if err != nil {
        panic(err.Error())
    }

	defer m.Close()

	n, err := db.Query("CREATE TABLE 'users' ('ID'	INTEGER,'Name'	TEXT,PRIMARY KEY('ID' AUTOINCREMENT));")

	if err != nil {
		panic(err.Error())
	}

	defer n.Close()
}


func (s *IMServiceImpl) Send(ctx context.Context, req *rpc.SendRequest) (*rpc.SendResponse, error) {
	timestamp := time.Now().Unix()
	// fmt.Println(timestamp)

	// req = SendRequest({Message:Message({Chat:John:Doe Text:Hello Sender:Joey:Tang SendTime:0})})
	// fmt.Println(req)

	req.Message.SetSendTime(timestamp)
	
	message := &rpc.Message{
		Chat: req.Message.GetChat(),
		Text: req.Message.GetText(),
		Sender: req.Message.GetSender(),
		SendTime: req.Message.GetSendTime(),
	}
	
	fmt.Println(message.SendTime)

	resp := rpc.NewSendResponse()
	resp.Code, resp.Msg = 0, "success"
	// fmt.Println("Handler")
	return resp, nil
}

func (s *IMServiceImpl) Pull(ctx context.Context, req *rpc.PullRequest) (*rpc.PullResponse, error) {
	resp := rpc.NewPullResponse()
	resp.Code, resp.Msg = areYouLucky()
	// fmt.Println("Handler")
	return resp, nil
}

func areYouLucky() (int32, string) {
	if rand.Int31n(2) == 1 {
		return 0, "success"
	} else {
		return 500, "oops"
	}
}
