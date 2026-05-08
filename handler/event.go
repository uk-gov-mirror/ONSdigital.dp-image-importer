package handler

// ImageUploaded provides an avro structure for an image uploaded event
type ImageUploaded struct {
	Path     string `avro:"path"`
	ImageID  string `avro:"image_id"`
	Filename string `avro:"filename"`
}
