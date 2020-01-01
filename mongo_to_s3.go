package main

import (
	"go.mongodb.org/mongo-driver/mongo"
	"github.com/aws/aws-sdk-go/aws/session"
	"go.mongodb.org/mongo-driver/mongo/options"
	"context"
	"log"
	"fmt"
	"net/http"
	"os"
	"github.com/aws/aws-sdk-go/aws"
	"go.mongodb.org/mongo-driver/bson"
	"time"
	"strings"
	"github.com/aws/aws-sdk-go/service/s3"
	"bytes"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"sync"
)

type mongoConnection func() *mongo.Client
type imageContent func(url string) *[]byte


// mongo connection return

var mongoConnect mongoConnection = func() *mongo.Client {
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(context.TODO(), clientOptions)

	if err != nil {
		log.Fatal(err)
	}
	err = client.Ping(context.TODO(), nil)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to MongoDB!")

	return client

}

var img imageContent = func(url string) *[]byte {

	res, err := http.Get(url)
	defer res.Body.Close()

	if err != nil {
		fmt.Println("********************************************************************************----Invalid url....")
	}

	if res.StatusCode == 200 || res.StatusCode == 201 {
		buffer := make([]byte, res.ContentLength)
		res.Body.Read(buffer)

		return &buffer

	}else {

		buffer := make([]byte, 0)
		return &buffer

	}

}


// Recovery func define which is recover from any panic conditions

func recovery() {

	if r := recover(); r != nil {fmt.Println("some error has occured_______________________________________________",r)}
}

const (
	S3_REGION = "ap-south-1"
	S3_BUCKET = "ttest-ts"
	DB = "square"
	COLLECTION = "india_curated_new"
)

type Project struct {
	ID string   `json:"id" bson:"_id"`
	Image []string `json:"psf_image" bson:"psf_images"`

}

type S3Handler struct {
	Session *session.Session
	// mongo collection passed
	Collection *mongo.Collection
}

// upload to s3 & update in mongodb
func (h *S3Handler) uploadToS3(p *Project, wg *sync.WaitGroup) {

	defer recovery()
	// get image content
	defer wg.Done()
	var arr []string

	for _, url := range(p.Image) {

		buffer:= img(url)
		if len(*buffer) > 0 {
			// upload to s3

			s := strings.Split(url, "/")
			url_key := s[len(s)-1]

			myurl := fmt.Sprintf("https://%s.s3.ap-south-1.amazonaws.com/%s", S3_BUCKET, url_key)

			_, err := s3.New(h.Session).PutObject(&s3.PutObjectInput{

				Bucket: aws.String(S3_BUCKET),
				Key:    aws.String(url_key),
				ACL:    aws.String("public-read"),
				Body:   bytes.NewReader(*buffer),
				ContentLength:        aws.Int64(int64(len(*buffer))),
				ContentType:          aws.String(http.DetectContentType(*buffer)),
				ContentDisposition:   aws.String("attachment"),
			})

			if err != nil{
				panic("********************************************************************************************************************************************")


			}else {
				arr = append(arr, myurl)
				//fmt.Println(akki)

			}
		}


	}

	// update to mongodb
	objID, _ := primitive.ObjectIDFromHex(p.ID)

	filter := bson.M{"_id": bson.M{"$eq": objID}}


	update := bson.M{
		"$set": bson.M{
			"new_image_urlss": arr,

		},
	}
	updateResult, _ := h.Collection.UpdateOne(context.TODO(), filter, update)

	fmt.Println(updateResult)


}


func main() {

	var wg sync.WaitGroup
	var result Project

	os.Setenv("AWS_ACCESS_KEY_ID", "############")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "###########")

	// create s3 session
	sess, err := session.NewSession(&aws.Config{Region: aws.String(S3_REGION)})

	c := mongoConnect()

	collection := c.Database(DB).Collection(COLLECTION)
	cur, err := collection.Find(context.Background(), bson.D{})
	if err != nil { log.Fatal(err) }
	defer cur.Close(context.Background())

	handler := &S3Handler{
		Session: sess,
		Collection: collection,
	}

	i := 0

	for cur.Next(context.Background()) {

		i = i+1

		err := cur.Decode(&result)
		if err != nil { log.Fatal(err) }

		if len(result.Image) > 0 {
			fmt.Println("--------------------------------------------------------------------------------------------------------", i)

			wg.Add(1)
			go handler.uploadToS3(&result, &wg)
			time.Sleep(time.Millisecond * 10)
		}
	}

	wg.Wait()



	fmt.Println("Connection to MongoDB closed.###########")
	err1 := c.Disconnect(context.TODO())

	if err1 != nil {
		log.Fatal(err1)
	}

	fmt.Println("________________________________Exist Main program....")








}

