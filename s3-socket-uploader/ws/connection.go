package ws

import (
	"log"
	"sync"

	"github.com/gofiber/websocket/v2"
)

var (
	connections = make(map[string]*websocket.Conn)
	mutex       = sync.RWMutex{}
)

func HandleConnection(c *websocket.Conn) {
	id := c.Params("id")

	mutex.Lock()
	connections[id] = c
	mutex.Unlock()

	defer func() {
		mutex.Lock()
		delete(connections, id)
		mutex.Unlock()
		c.Close()
	}()

	for {
		if _, _, err := c.ReadMessage(); err != nil {
			break
		}
	}
}

func GetConnection(id string) *websocket.Conn {
	mutex.RLock()
	defer mutex.RUnlock()
	return connections[id]
}

func SendJSON(c *websocket.Conn, data interface{}) {
	if c != nil {
		if err := c.WriteJSON(data); err != nil {
			log.Println("WebSocket send error:", err)
		}
	}
}
