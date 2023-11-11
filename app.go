package main

import (
	"fmt"
	gocqlastra "github.com/datastax/gocql-astra"
	"github.com/gocql/gocql"
	"log"
	"math/rand"
	"os"
	"time"
)

type Book struct {
	ID     gocql.UUID
	Title  string
	Author string
	Year   int
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func main() {

	var err error

	var cluster *gocql.ClusterConfig

	fmt.Println("Creating the cluster now")

	/*
		cluster, err = gocqlastra.NewClusterFromURL("https://api.astra.datastax.com", os.Getenv("ASTRA_DB_ID"), os.Getenv("ASTRA_DB_APPLICATION_TOKEN"), 20*time.Second)
		if err != nil {
			fmt.Errorf("unable to load cluster %s from astra: %v", os.Getenv("ASTRA_DB_APPLICATION_TOKEN"), err)
		}
	*/

	cluster, err = gocqlastra.NewClusterFromBundle(os.Getenv("ASTRA_DB_SECURE_BUNDLE_PATH"),
		os.Getenv("ASTRA_DB_CLIENT_ID"), os.Getenv("ASTRA_DB_SECRET"), 30*time.Second)

	cluster.Timeout = 30 * time.Second
	session, err := gocql.NewSession(*cluster)

	if err != nil {
		log.Fatalf("unable to connect session: %v", err)
	}

	defer session.Close()

	// Delete the table
	if err := session.Query(`DROP TABLE IF EXISTS demo.books`).Exec(); err != nil {
		fmt.Println("Dropping table to clean for examples.")
	}

	// Create the table
	// fmt.Println("Creating the table now")
	if err := session.Query(`CREATE TABLE demo.books ( id uuid PRIMARY KEY, title text, author text, year int );`).Exec(); err != nil {
		log.Fatal(err)
	}

	i := 0
	for i < 100000 {
		// fmt.Println("Starting iteration ", i)
		// Create Rows
		newBook := Book{
			ID:     gocql.TimeUUID(),
			Title:  randSeq(10),
			Author: randSeq(10),
			Year:   rand.Intn(2050),
		}

		// fmt.Println("Creating book ", newBook.ID)

		if err := session.Query(`INSERT INTO demo.books (id, title, author, year) VALUES (?, ?, ?, ?)`,
			newBook.ID, newBook.Title, newBook.Author, newBook.Year).Exec(); err != nil {
			log.Fatal(err)
		}

		// fmt.Println("Querying the books now:")
		iter := session.Query(`SELECT id, title, author, year FROM demo.books limit 10`).Iter()

		var books []Book
		var book Book
		for iter.Scan(&book.ID, &book.Title, &book.Author, &book.Year) {
			books = append(books, book)
		}

		if err := iter.Close(); err != nil {
			log.Fatal(err)
		}

		// Print all books
		//for _, book := range books {
		// fmt.Println("Book:", book)
		//}

		// Delete
		// fmt.Println("Deleting book %s ", newBook.ID)
		if err := session.Query(`DELETE FROM demo.books WHERE id = ?`, newBook.ID).Exec(); err != nil {
			log.Fatal(err)
		}

		time.Sleep(10 * time.Millisecond)
		i++
	}
}
