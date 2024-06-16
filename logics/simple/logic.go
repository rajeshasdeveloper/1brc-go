package simple

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
)

type weatherMetadata struct {
	Min   float64
	Max   float64
	Sum   float64
	Count float64
}

func SimpleLogic(inputFilePath, outputFilePath string) {
	//file, _ := os.Open("data/measurements.txt")
	//defer file.Close()

	fileData, err := os.ReadFile(inputFilePath)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("processing %d bytes of file\n", len(fileData))

	var weatherMap = make(map[string]*weatherMetadata)

	stringifiedWeatherData := strings.Split(string(fileData), "\n")

	for _, weatherData := range stringifiedWeatherData {
		weatherStationName, temperature, ok := strings.Cut(weatherData, ";")
		if !ok {
			continue
		}

		f, err := strconv.ParseFloat(temperature, 64)
		if err != nil {
			log.Println(err)
		}

		existingWeatherData, ok := weatherMap[weatherStationName]
		if !ok {
			weatherMap[weatherStationName] = &weatherMetadata{
				Min:   f,
				Max:   f,
				Sum:   f,
				Count: 1,
			}
		} else {
			existingWeatherData.Min = min(f, existingWeatherData.Min)
			existingWeatherData.Max = max(f, existingWeatherData.Max)
			existingWeatherData.Sum += f
			existingWeatherData.Count += 1
		}
	}

	var weatherStationNames []string

	for key, _ := range weatherMap {
		weatherStationNames = append(weatherStationNames, key)
	}

	sort.Strings(weatherStationNames)

	file, err := os.Create(outputFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	for _, key := range weatherStationNames {
		_, err := file.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f\n", key, weatherMap[key].Min, weatherMap[key].Max, weatherMap[key].Sum/weatherMap[key].Count))
		if err != nil {
			log.Println(err)
		}
	}
}

func SimpleLogicWithBuffer(inputFilePath, outputFilePath string) {

	dataFile, err := os.Open(inputFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer dataFile.Close()

	var weatherResultMap = make(map[string]*weatherMetadata)
	fileScanner := bufio.NewScanner(dataFile)
	fileScanner.Split(bufio.ScanLines)

	for fileScanner.Scan() {
		//NOTE: Converting bytes to string here increases latency
		//rawString := fileScanner.Text()
		//weatherStationName, temperature, ok := strings.Cut(rawString, ";")

		//NOTE:
		weatherStationName, temperature, ok := bytes.Cut(fileScanner.Bytes(), []byte(";"))
		if !ok {
			log.Println("not ok")
			continue
		}

		//temperatureValue, err := strconv.ParseFloat(temperature, 64)

		temperatureValue, err := strconv.ParseFloat(string(temperature), 64)
		if err != nil {
			log.Println(err)
			return
		}

		//existingData, ok := weatherResultMap[weatherStationName]

		existingData, ok := weatherResultMap[string(weatherStationName)]

		if ok {
			existingData.Max = max(existingData.Max, temperatureValue)
			existingData.Min = min(existingData.Min, temperatureValue)
			existingData.Sum += temperatureValue
			existingData.Count += 1
		} else {

			//weatherResultMap[weatherStationName] = &weatherMetadata{

			weatherResultMap[string(weatherStationName)] = &weatherMetadata{
				Min:   temperatureValue,
				Max:   temperatureValue,
				Sum:   temperatureValue,
				Count: 1,
			}
		}
	}

	var weatherStationNames = make([]string, 0, len(weatherResultMap))

	for stationName, _ := range weatherResultMap {
		weatherStationNames = append(weatherStationNames, stationName)
	}
	slices.Sort(weatherStationNames)
	fmt.Print("{")
	for idx, stationName := range weatherStationNames {

		fmt.Printf("%s=%.1f/%.1f/%.1f", stationName, weatherResultMap[stationName].Min, weatherResultMap[stationName].Max, weatherResultMap[stationName].Sum/weatherResultMap[stationName].Count)
		if idx < len(weatherStationNames)-1 {
			fmt.Print(", ")
		}
	}
	fmt.Print("}\n")
}
