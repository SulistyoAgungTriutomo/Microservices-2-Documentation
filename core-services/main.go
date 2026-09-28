package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type PaymentMessage struct {
	PaymentID string `json:"payment_id"`
	UserID    string `json:"user_id"`
	Amount    int    `json:"amount"`
	CreatedAt string `json:"created_at"`
}

var rabbitChannel *amqp.Channel

func initRabbitMQ() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Gagal terhubung ke RabbitMQ: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Gagal membuka channel RabbitMQ: %v", err)
	}

	// 1. Declare Direct Exchange
	err = ch.ExchangeDeclare(
		"payment.direct", // exchange name
		"direct",         // type
		true,             // durable
		false,            // auto-deleted
		false,            // internal
		false,            // no-wait
		nil,
	)
	if err != nil {
		log.Fatalf("Gagal membuat exchange: %v", err)
	}

	// 2. Declare Queue
	q, err := ch.QueueDeclare(
		"payment.queue", // queue name
		true,            // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		nil,
	)
	if err != nil {
		log.Fatalf("Gagal membuat queue: %v", err)
	}

	err = ch.QueueBind(
		q.Name,
		"payment.key",
		"payment.direct",
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Gagal melakukan queue binding: %v", err)
	}

	rabbitChannel = ch
	log.Println("[Core-Service] Berhasil terhubung ke RabbitMQ")
}

func main() {
	initRabbitMQ()

	r := gin.Default()

	r.POST("/transactions/pay", func(c *gin.Context) {
		var req struct {
			UserID string `json:"user_id" binding:"required"`
			Amount int    `json:"amount" binding:"required"`
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Payload tidak valid"})
			return
		}

		// Buat pesan pembayaran
		paymentMsg := PaymentMessage{
			PaymentID: uuid.New().String(),
			UserID:    req.UserID,
			Amount:    req.Amount,
			CreatedAt: time.Now().Format(time.RFC3339),
		}

		body, _ := json.Marshal(paymentMsg)

		err := rabbitChannel.Publish(
			"payment.direct",
			"payment.key",
			false,
			false,
			amqp.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp.Persistent,
				Body:         body,
			},
		)

		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Gagal mengirim pembayaran ke antrean"})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status":     "pending",
			"payment_id": paymentMsg.PaymentID,
			"message":    "Pembayaran sedang diproses secara asinkron",
		})
	})

	r.Run(":8080")
}
