package handlers

import (
	"context"
	"frogo/utils"
	"frogo/ws"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gofiber/fiber/v2"
)

func UploadHandler(c *fiber.Ctx) error {
	id := c.Params("id")
	conn := ws.GetConnection(id)
	if conn == nil {
		return fiber.NewError(fiber.StatusBadRequest, "WebSocket not connected")
	}

	form, err := c.MultipartForm()
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, "Invalid form data")
	}

	files := form.File["files"]
	result := make(map[string]string)

	for _, file := range files {
		src, err := file.Open()
		if err != nil {
			result[file.Filename] = "open error"
			ws.SendJSON(conn, fiber.Map{"file": file.Filename, "status": "open_error"})
			continue
		}

		key := "uploads/" + file.Filename
		_, err = utils.S3Client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: &utils.Bucket,
			Key:    &key,
			Body:   src,
		})

		if err != nil {
			result[file.Filename] = "upload failed"
			ws.SendJSON(conn, fiber.Map{"file": file.Filename, "status": "error"})
		} else {
			result[file.Filename] = "uploaded"
			ws.SendJSON(conn, fiber.Map{"file": file.Filename, "status": "success"})
		}
	}

	return c.JSON(result)
}
