package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
    "math/rand"
	"strings"
	"os/exec"
	"sort"

	"github.com/segmentio/kafka-go"
)

const (
	topic         = "avg"
	brokerAddress = "0:29092"
)

func main() {
	ctx := context.Background()

	go produce(ctx)
	consume(ctx)
}

func getRandomString(length int) string {
	charSet := "abcdedfghijklmnopqrstuvwxyz"
	var output strings.Builder
    for i := 0; i < length; i++ {
        random := rand.Intn(len(charSet))
        randomChar := charSet[random]
        output.WriteString(string(randomChar))
	}
	return output.String()
}


func produce(ctx context.Context) {
	
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
	})

	for {
		i := rand.Intn(100)
		c := getRandomString(1)

		err := w.WriteMessages(ctx, kafka.Message{
			Key: []byte(c),
			Value: []byte(fmt.Sprintf("%v,%v,",c ,strconv.Itoa(i))),
		})
		if err != nil {
			log.Println("could not write message " + err.Error())
			continue
		}
	}
}


func clear() {
	cmd := exec.Command("clear")
	cmd.Stdout = os.Stdout
	cmd.Run()
}

func consume(ctx context.Context) {

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		GroupID: "kth-group",
	})

	
	allData := Items{}
	fmt.Println("> Please wait ...")
	time.Sleep(1*time.Second)
	fmt.Println("> Processing messages")
	for {
		msg, err := r.ReadMessage(ctx)

		if err != nil {
			log.Println("could not read message " + err.Error())
			continue
		}
		s := strings.Split(string(msg.Value), ",")
		character, num  := s[0], s[1]

		allData = allData.Add(character, num)
		sort.Slice(allData, func(i, j int) bool {
			return allData[i].Count > allData[j].Count
		})
		clear()
		fmt.Println("======================== KTH REPORT ===========================")
		fmt.Printf("================ %v =================", time.Now().Format("01-02-2006 15:04:05 Monday"))
		fmt.Println()
		fmt.Println()
		fmt.Println("Token|  Sum    Count   Average  ")
		fmt.Println("---------------------------------------------------------------")
		for _, data := range allData {
			fmt.Printf("  %v  |  %v      %v        %v", data.Token, data.Sum, data.Count, data.Average)
			fmt.Println()
			fmt.Println("---------------------------------------------------------------")
		}
	}
}

// Item struct
type Item struct {
	Token string
	Sum float64
	Count int
	Average float64
}

// Items list
type Items []Item

// Add function with Item receiver
func (items Items) Add(character, num string) []Item {
	for k, v := range items {
		if strings.ToLower(v.Token) == strings.ToLower(character) {
			tmp, _ := strconv.Atoi(num)
			sum := v.Sum + float64(tmp)
			count := v.Count + 1
			avg := sum/float64(count)

			items[k] = Item{
				Token: v.Token,
				Sum: sum,
				Count: count,
				Average: avg,
			}
			return items
		}
	}

	tmp, _ := strconv.Atoi(num)
	items = append(items, Item{
		Token: character,
		Sum: float64(tmp),
		Count: 1,
		Average: float64(tmp),
		
	})
	return items
}