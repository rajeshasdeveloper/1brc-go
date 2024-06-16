package advanced

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/edsrzf/mmap-go"
)

type weatherMetadata struct {
	StationName string
	Min         int16
	Max         int16
	Sum         int32
	Count       int16
}

type splittedDataStruct struct {
	start int
	end   int
}

func CustomMmapImplementation(inputFilePath, outputFilePath string) {

	// opening the file
	f, err := os.Open(inputFilePath)
	if err != nil {
		log.Println(err)
		return
	}
	defer f.Close()

	// mmap ing the file
	data, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		log.Println(err)
		return
	}
	defer data.Unmap()

	// getting the total length of []byte (mmap return []byte)
	length := len(data)
	fmt.Printf("processing %d bytes of file...", length)
	fmt.Print("\n")

	//var count int
	var stationName = ""
	var temperature int16
	var stationNameStartIndex int
	var weatherDataMap = make(map[string]*weatherMetadata)

	for i := 0; i < length; i++ {
		if data[i] == ';' {
			stationName = string(data[stationNameStartIndex:i])
			temperature = 0
			i += 1
			negative := false

			for data[i] != '\n' {
				ch := data[i]
				if ch == '.' {
					i += 1
					continue
				}
				if ch == '-' {
					negative = true
					i += 1
					continue
				}

				ch -= '0'
				if ch > 9 {
					panic(fmt.Sprintf("invalid character %v", ch))
				}
				temperature = temperature + int16(ch*10)
				i += 1

				if i >= length {
					break
				}
			}

			if negative {
				temperature = -temperature
			}

			stationData := weatherDataMap[stationName]
			if stationData == nil {
				weatherDataMap[stationName] = &weatherMetadata{
					Min:   temperature,
					Max:   temperature,
					Sum:   int32(temperature),
					Count: 1,
				}
			} else {
				stationData.Min = min(temperature, stationData.Min)
				stationData.Max = max(temperature, stationData.Min)
				stationData.Count += 1
				stationData.Sum = stationData.Sum + int32(temperature)
			}

			stationName = ""
			temperature = 0
		}

		if i >= length {
			break
		}

		if data[i] == '\n' {
			stationNameStartIndex = i + 1
		}

	}

	fmt.Println(len(weatherDataMap))
}

func CustomMmapWithParallelImplementation(inputFilePath, outputFilePath string) {

	// opening the file
	f, err := os.Open(inputFilePath)
	if err != nil {
		log.Println(err)
		return
	}
	defer f.Close()

	// mmap ing the file
	data, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		log.Println(err)
		return
	}
	defer data.Unmap()

	// getting the total length of []byte (mmap return []byte)
	length := len(data)
	fmt.Printf("processing %d bytes of file...", length)
	fmt.Print("\n")

	var splittedBytesData = make([]splittedDataStruct, 0, 16)
	var noOfBytesForEachProcessor = length / 16
	var startIndexOfNextByte = 0
	var endIndexOfNextByte = noOfBytesForEachProcessor
	var weatherDataChan = make(chan *weatherMetadata, length)
	var weatherDataMap = make(map[string]*weatherMetadata)
	var wg sync.WaitGroup

	for i := 1; i <= 16; i++ {
		if endIndexOfNextByte >= length {
			splittedBytesData = append(splittedBytesData, splittedDataStruct{
				start: startIndexOfNextByte,
				end:   length,
			})
			break
		}
		splittedDataLastByte := data[endIndexOfNextByte]
		if splittedDataLastByte == '\n' {
			splittedBytesData = append(splittedBytesData, splittedDataStruct{
				start: startIndexOfNextByte,
				end:   endIndexOfNextByte,
			})
			startIndexOfNextByte = endIndexOfNextByte + 1
			endIndexOfNextByte += noOfBytesForEachProcessor
			continue
		}
		endIndexOfNextByte += 1
		for {
			splittedDataLastByte = data[endIndexOfNextByte]
			if splittedDataLastByte == '\n' {
				splittedBytesData = append(splittedBytesData, splittedDataStruct{
					start: startIndexOfNextByte,
					end:   endIndexOfNextByte,
				})
				startIndexOfNextByte = endIndexOfNextByte + 1
				endIndexOfNextByte += noOfBytesForEachProcessor
				break
			}
			endIndexOfNextByte += 1
			if endIndexOfNextByte >= length {
				break
			}
		}
	}
	// fmt.Println(splittedBytesData)
	fmt.Println(len(splittedBytesData))
	// os.Exit(1)
	splittedBytesData = splittedBytesData[0:1]
	for _, bytesData := range splittedBytesData {
		wg.Add(1)
		go processDataBytes(&data, bytesData, weatherDataChan, &wg)
	}
	wg.Wait()
	close(weatherDataChan)

	for msg := range weatherDataChan {
		stationData := weatherDataMap[msg.StationName]
		if stationData == nil {
			weatherDataMap[msg.StationName] = &weatherMetadata{
				Min:   msg.Min,
				Max:   msg.Max,
				Sum:   msg.Sum,
				Count: 1,
			}
		} else {
			stationData.Min = min(msg.Min, stationData.Min)
			stationData.Max = max(msg.Max, stationData.Min)
			stationData.Count += 1
			stationData.Sum = stationData.Sum + msg.Sum
		}
	}
	// fmt.Println(weatherDataMap)
}

func processDataBytes(b *mmap.MMap, indexMetaData splittedDataStruct, weatherDataChannel chan *weatherMetadata, wg *sync.WaitGroup) {

	// fmt.Printf("started processing the %d bytes")
	bData := *b
	data := bData[indexMetaData.start:indexMetaData.end]
	var stationNameStartIndex int
	var bytesLength = indexMetaData.end - indexMetaData.start
	for i := 0; i < bytesLength; i++ {
		if data[i] == ';' {
			// calculating station name
			stationName := string(data[stationNameStartIndex:i])
			var temperature int16 = 0
			negative := false
			i += 1

			// calculating temperature
			for data[i] != '\n' {
				ch := data[i]
				if ch == '-' {
					negative = true
					i += 1
					continue
				} else if ch == '.' {
					i += 1
					continue
				}

				ch -= '0'
				if ch > 9 {
					panic(fmt.Sprintf("invalid character %v", ch))
				}
				temperature = temperature*10 + int16(ch)
				i += 1

				if i >= bytesLength {
					break
				}
			}

			if negative {
				temperature = -temperature
			}

			weatherDataToSend := &weatherMetadata{
				StationName: stationName,
				Min:         temperature,
				Max:         temperature,
				Sum:         int32(temperature),
				Count:       1,
			}

			// sending weather data to main goroutine
			weatherDataChannel <- weatherDataToSend
		}

		if i >= bytesLength {
			break
		}

		if data[i] == '\n' {
			stationNameStartIndex = i + 1
		}
	}
	wg.Done()

	return
}
