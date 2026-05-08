package schema

import (
	"github.com/ONSdigital/dp-kafka/v5/avro"
)

var imageUploadedEvent = `{
  "type": "record",
  "name": "image-uploaded",
  "fields": [
    {"name": "image_id", "type": "string", "default": ""},
    {"name": "path", "type": "string", "default": ""},
    {"name": "filename", "type": "string", "default": ""}
  ]
}`

// ImageUploadedEvent is the Avro schema for Image uploaded messages.
var ImageUploadedEvent = &avro.Schema{
	Definition: imageUploadedEvent,
}
