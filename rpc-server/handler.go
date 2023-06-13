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
var count = 0

func InitializeDB() {
	// fmt.Println('0')
	var err error
	db, err = Connection()
	if err != nil {
		log.Println(err.Error())
	} else {
		CreateTable()
	}
}

func Connection() (*sql.DB, error) {
	var db *sql.DB
	var err error

	// Retry connecting to the MySQL server with exponential backoff
	err = retry.Do(
		func() error {
			db, err = sql.Open("mysql", "docker:password@tcp(mysql:3306)/messages")
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

func CreateTable () {
	var err error

	dropTableSQL1 := "DROP TABLE IF EXISTS messages"
	_, err = db.Exec(dropTableSQL1)
	if err != nil {
		panic(err.Error())
	}

	createTableSQL1 := `
    CREATE TABLE IF NOT EXISTS messages (
		id INT AUTO_INCREMENT PRIMARY KEY,
		chat INT NOT NULL,
		sender INT NOT NULL,
        text VARCHAR(100) NOT NULL,
        sendtime INT NOT NULL
	)`

    _, err = db.Exec(createTableSQL1)
    if err != nil {
        panic(err.Error())
    }

    fmt.Println("Messages created successfully")

	dropTableSQL2 := "DROP TABLE IF EXISTS users"
	_, err = db.Exec(dropTableSQL2)
	if err != nil {
		panic(err.Error())
	}

	createTableSQL2 := `
    CREATE TABLE IF NOT EXISTS users (
		id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(50) NOT NULL
	)`

	_, err = db.Exec(createTableSQL2)
    if err != nil {
        panic(err.Error())
    }

    fmt.Println("Users created successfully")
}


func CheckUser (name string) int32 {
	row := db.QueryRow("SELECT id FROM users WHERE Name = ?", name)

	var userID int32
    err := row.Scan(&userID)

	if err == sql.ErrNoRows {
		// User does not exist
		// fmt.Println("User does not exist")

		insertSQL := "INSERT INTO users (name) VALUES (?)"
		_, err := db.Exec(insertSQL, name)
		if err != nil {
			// handle error
			return -1
			// fmt.Println("Error occured")
		} else {
			return CheckUser(name)
			// fmt.Println(name + " inserted successfully.")
		}

	} else if err != nil {
		// Error occurred while querying the database
		return -1
		// fmt.Println("Error occured")
	} else {
		return userID
		// fmt.Println("User exists")
	}
}

func CheckMessages () {
	row, err := db.Query("SELECT ID, chat, text, sender, sendtime FROM messages")

	if err != nil {
		fmt.Println("Error")
	}

	var id, chat, sender, sendtime int32
	var text string
	for row.Next() {
		err := row.Scan(&id, &chat, &text, &sender, &sendtime)
		if err != nil {
			fmt.Println("Error")
		}
		fmt.Println(id, chat, text, sender, sendtime)
	}
}

func CheckUsers () {
	row, err := db.Query("SELECT ID, name FROM users")

	if err != nil {
		fmt.Println("Error")
	}

	var id int32
	var name string
	for row.Next() {
		err := row.Scan(&id, &name)
		if err != nil {
			fmt.Println("Error")
		}
		fmt.Println(id, name)
	}
}


func (s *IMServiceImpl) Send(ctx context.Context, req *rpc.SendRequest) (*rpc.SendResponse, error) {
	if count == 0 {
		InitializeDB()
		count = 1
	}
	
	timestamp := time.Now().Unix()
	// fmt.Println(timestamp)

	// req = SendRequest({Message:Message({Chat:John:Doe Text:Hello Sender:Joey:Tang SendTime:0})})
	// fmt.Println(req)

	req.Message.SetSendTime(timestamp)

	// fmt.Println(req.Message.GetSendTime())
	
	message := &rpc.Message{
		Chat: req.Message.GetChat(),
		Text: req.Message.GetText(),
		Sender: req.Message.GetSender(),
		SendTime: req.Message.GetSendTime(),
	}

	var receiver, sender int32
	receiver = CheckUser(message.Chat)
	sender = CheckUser(message.Sender)

	// fmt.Println(receiver, reflect.TypeOf(receiver))
	// fmt.Println(sender, reflect.TypeOf(sender))
	// fmt.Println(message.Text, reflect.TypeOf(message.Text))
	// fmt.Println(message.SendTime, reflect.TypeOf(message.SendTime))

	insertSQL := "INSERT INTO messages (chat, sender, text, sendtime) VALUES (?, ?, ?, ?)"
	_, err := db.Exec(insertSQL, receiver, sender, message.Text, message.SendTime)

	if err != nil {
		// handle error
		fmt.Println("Error occured", err.Error())
	} else {
		fmt.Println("Message inserted successfully.")
	}

	// fmt.Println(message.SendTime)

	resp := rpc.NewSendResponse()
	resp.Code, resp.Msg = 0, "success"
	// fmt.Println("Handler")
	return resp, nil
}

func (s *IMServiceImpl) Pull(ctx context.Context, req *rpc.PullRequest) (*rpc.PullResponse, error) {
	if count == 0 {
		InitializeDB()
		count = 1
	}

	// CheckMessages()
	// CheckUsers()
	
	resp := rpc.NewPullResponse()
	resp.Code, resp.Msg = 0, "success"

	var chat string
	var cursor int64
	var limit int32
	var reverse bool
	chat = req.GetChat()
	cursor = req.GetCursor()
	limit = req.GetLimit()
	reverse = req.GetReverse()

	var receiver int32
	receiver = CheckUser(chat) 
	// "SELECT messages.text, users.name, messages.sendtime FROM messages, users WHERE messages.sender = users.id AND messages.chat = ? AND messages.sendtime >= ? ORDER BY messages.sendtime DESC LIMIT ?", receiver, cursor, limit

	var results *sql.Rows
	var err error

	if reverse {
		results, err = db.Query("SELECT messages.text, users.name, messages.sendtime FROM messages, users WHERE messages.sender = users.id AND messages.chat = ? AND messages.sendtime >= ? ORDER BY messages.sendtime DESC LIMIT ?", receiver, cursor, limit)
	} else {
		results, err = db.Query("SELECT messages.text, users.name, messages.sendtime FROM messages, users WHERE messages.sender = users.id AND messages.chat = ? AND messages.sendtime >= ? ORDER BY messages.sendtime ASC LIMIT ?", receiver, cursor, limit)
	}

    if err != nil {
        panic(err.Error()) // proper error handling instead of panic in your app
    }

	var array [] *rpc.Message

    for results.Next() {
		var text string
		var name string
		var sendtime int64
	
		err = results.Scan(&text, &name, &sendtime)
		if err != nil {
			// handle error
			fmt.Println("Error occurred:", err)
			return nil, err
		}
	
		// Process the values retrieved from the row
		array = append(array, &rpc.Message{
			Chat: chat,
			Text: text,
			Sender: name,
			SendTime: sendtime,
		})
		// fmt.Println(array)
	}

	resp.SetMessages(array)

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
