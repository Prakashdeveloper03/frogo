package main

import (
	"log"

	"frogo/handlers"
	"frogo/utils"
	"frogo/ws"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/websocket/v2"
)

func main() {
	utils.InitAWS()
	app := fiber.New()
	app.Use(logger.New())
	app.Get("/ws/:id", websocket.New(ws.HandleConnection))
	app.Post("/upload/:id", handlers.UploadHandler)

	log.Fatal(app.Listen(":3000"))
}
