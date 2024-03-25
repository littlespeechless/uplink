// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	//"storj.io/uplink"
	"storj.io/uplink"
	"storj.io/uplink/edge"

	AwesomeHook "storj.io/uplink/hook"
)

const defaultExpiration = 7 * 24 * time.Hour

// UploadAndDownloadData uploads the specified data to the specified key in the
// specified bucket, using the specified Satellite, API key, and passphrase.
func UploadAndDownloadData(ctx context.Context,
	accessGrant, bucketName, uploadKey string, dataToUpload []byte) error {

	// Parse access grant, which contains necessary credentials and permissions.
	access, err := uplink.ParseAccess(accessGrant)
	if err != nil {
		return fmt.Errorf("could not request access grant: %v", err)
	}

	// Open up the Project we will be working with.
	project, err := uplink.OpenProject(ctx, access)
	if err != nil {
		return fmt.Errorf("could not open project: %v", err)
	}
	defer project.Close()

	// Ensure the desired Bucket within the Project is created.
	_, err = project.EnsureBucket(ctx, bucketName)
	if err != nil {
		return fmt.Errorf("could not ensure bucket: %v", err)
	}

	// Intitiate the upload of our Object to the specified bucket and key.
	upload, err := project.UploadObject(ctx, bucketName, uploadKey, &uplink.UploadOptions{
		// It's possible to set an expiration date for data.
		Expires: time.Now().Add(defaultExpiration),
	})
	if err != nil {
		return fmt.Errorf("could not initiate upload: %v", err)
	}

	// Copy the data to the upload.
	buf := bytes.NewBuffer(dataToUpload)
	_, err = io.Copy(upload, buf)
	if err != nil {
		_ = upload.Abort()
		return fmt.Errorf("could not upload data: %v", err)
	}

	// Commit the uploaded object.
	// this will fail due to modification with fake upload
	err = upload.Commit()
	if err != nil {
		return fmt.Errorf("could not commit uploaded object: %v", err)
	}

	return nil
}

func CreatePublicSharedLink(ctx context.Context, accessGrant, bucketName, objectKey string) (string, error) {
	// Define configuration for the storj sharing site.
	config := edge.Config{
		AuthServiceAddress: "auth.storjshare.io:7777",
	}

	// Parse access grant, which contains necessary credentials and permissions.
	access, err := uplink.ParseAccess(accessGrant)
	if err != nil {
		return "", fmt.Errorf("could not parse access grant: %w", err)
	}

	// Restrict access to the specified paths.
	restrictedAccess, err := access.Share(
		uplink.Permission{
			// only allow downloads
			AllowDownload: true,
			// this allows to automatically cleanup the access grants
			NotAfter: time.Now().Add(defaultExpiration),
		}, uplink.SharePrefix{
			Bucket: bucketName,
			Prefix: objectKey,
		})
	if err != nil {
		return "", fmt.Errorf("could not restrict access grant: %w", err)
	}

	// RegisterAccess registers the credentials to the linksharing and s3 sites.
	// This makes the data publicly accessible, see the security implications in https://docs.storj.io/dcs/concepts/access/access-management-at-the-edge.
	credentials, err := config.RegisterAccess(ctx, restrictedAccess, &edge.RegisterAccessOptions{Public: true})
	if err != nil {
		return "", fmt.Errorf("could not register access: %w", err)
	}

	// Create a public link that is served by linksharing service.
	url, err := edge.JoinShareURL("https://link.storjshare.io", credentials.AccessKeyID, bucketName, objectKey, nil)
	if err != nil {
		return "", fmt.Errorf("could not create a shared link: %w", err)
	}

	return url, nil
}

func crawl(ctx context.Context, accessGrant *string, bucketName, objectKey string) {
	// generate random bytes
	size := 64 * 1024 * 1024 // 64MB
	data := make([]byte, size)
	_, err := rand.Read(data)
	for !AwesomeHook.ShouldStopDict[bucketName] {
		err = UploadAndDownloadData(ctx, *accessGrant, bucketName, objectKey, data)
		if err != nil {
			fmt.Println(err)
		}
	}
}
func main() {
	// parse args
	NAAccessGrant := flag.String("na", os.Getenv("ACCESS_GRANT"), "access grant from satellite")
	EUAccessGrant := flag.String("eu", os.Getenv("ACCESS_GRANT"), "access grant from satellite")
	APAccessGrant := flag.String("ap", os.Getenv("ACCESS_GRANT"), "access grant from satellite")
	outputPath := flag.String("output", "./", "output path for saving the result")
	flag.Parse()

	// create log folder to store all logs
	saveDir := *outputPath
	err := os.MkdirAll(saveDir, 0755)
	if err != nil {
		panic("Error creating logs folder. Exiting")
	}
	// setup log name
	currentDate := time.Now().Format("2006-01-02_15-04")
	fmt.Println(currentDate)
	//os.Exit(0)
	fileName := fmt.Sprintf("%s.log", currentDate)
	savePath := filepath.Join(saveDir, fileName)
	jsonPath := filepath.Join(saveDir, fmt.Sprintf("%s.json", currentDate))
	// create log file
	file, err := os.OpenFile(savePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	// redirect output
	log.SetOutput(file)

	ctx := context.Background()

	bukectAccessGrant := make(map[string]*string)
	bukectAccessGrant["na-bucket"] = NAAccessGrant
	bukectAccessGrant["eu-bucket"] = EUAccessGrant
	bukectAccessGrant["ap-bucket"] = APAccessGrant

	AwesomeHook.ShouldStopDict["na-bucket"] = false
	AwesomeHook.ShouldStopDict["eu-bucket"] = false
	AwesomeHook.ShouldStopDict["ap-bucket"] = false

	objectKey := "foo/bar/baz"
	// run crawl in parallel
	var wg sync.WaitGroup
	for _, bucketName := range []string{"na-bucket", "eu-bucket", "ap-bucket"} {
		wg.Add(1)
		go func(bucketName string) {
			defer wg.Done()
			// generate random bytes
			size := 64 * 1024 * 1024 // 64MB
			data := make([]byte, size)
			_, err := rand.Read(data)
			for !AwesomeHook.ShouldStopDict[bucketName] {
				err = UploadAndDownloadData(ctx, *bukectAccessGrant[bucketName], bucketName, objectKey, data)
				if err != nil {
					//fmt.Println(err)
				}
				time.Sleep(1 * time.Second)
			}
		}(bucketName)
	}
	wg.Wait()
	//err = UploadAndDownloadData(ctx, *accessGrant, bucketName, objectKey, data)
	//time.Sleep(1 * time.Second)
	// save dict to json

	saveDictToJson(AwesomeHook.Dict, jsonPath)

	fmt.Println("success!")

}

func saveDictToJson(dict map[string]string, path string) {
	jsonData, err := json.Marshal(dict)
	if err != nil {
		fmt.Println(err)
		return
	}

	file, err := os.Create(path)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	_, err = file.Write(jsonData)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("JSON data written to ", file.Name())
}
