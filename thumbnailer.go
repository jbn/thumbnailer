// Creates thumbnails of all images in a directory.
package main

import (
    "bytes"
    "strings"
    "path/filepath"
    "flag"
    "fmt"
    "github.com/disintegration/gift"
    "hash/crc32"
    "image"
    "image/png"
    "io"
    "log"
    "os"
    "time"
    "runtime"
    "sync"
    "math/rand"
    _ "image/gif"
    _ "image/jpeg"
)

//=============================================================================

var inputDir     = flag.String("i", "image_packs", "input directory")
var outputDir    = flag.String("o", "image_thumbs", "output directory")
var deduplicate  = flag.Bool("d", true, "skip duplicates")
var shufflePaths = flag.Bool("s", true, "shuffle image paths")
var flipVertical = flag.Bool("f", true, "flip vertical")

var flipOps      = []bool{false}

//=============================================================================

var wg sync.WaitGroup

var RECEPTOR_FIELD = []int{224, 224}

var ANCHORINGS = map[string]gift.Anchor{
    "left": gift.LeftAnchor,
    "right": gift.RightAnchor,
    "center": gift.CenterAnchor,
}

//=============================================================================

func readImage(path string) (image.Image, int64, error) {
    fp, err := os.Open(path)
    defer fp.Close()
    if err != nil {
        return nil, -1, err
    }

    buf := bytes.NewBuffer(nil)
    io.Copy(buf, fp)
    checksum := crc32.ChecksumIEEE(buf.Bytes())

    img, _, err := image.Decode(buf)
    if err != nil {
        return nil, -1, err
    }

    return img, int64(checksum), nil
}


func calcResizeBounds(src image.Image) (int, int) {
    bounds := src.Bounds()

    x, y := bounds.Max.X, bounds.Max.Y

    if x < y {
        s := RECEPTOR_FIELD[0] / x
        return RECEPTOR_FIELD[0], y * s
    } else {
        s := RECEPTOR_FIELD[1] / y
        return x * s, RECEPTOR_FIELD[1]
    }
}

func subImage(src image.Image) image.Image {
    x, y := calcResizeBounds(src)
    g := gift.New(gift.Resize(x, y, gift.LanczosResampling))
    dst := image.NewNRGBA(g.Bounds(src.Bounds()))
    g.Draw(dst, src)
    return dst
}


func createThumb(src image.Image, anchors map[string]gift.Anchor) map[string]image.Image {
    thumbs := make(map[string]image.Image)

    // Precalculate the sub-image.
    src = subImage(src)

    for k, anchor := range anchors {
        for _, flipped := range flipOps {
            outputName := k

            filters := []gift.Filter{gift.CropToSize(RECEPTOR_FIELD[0], RECEPTOR_FIELD[1], anchor)}
            if flipped {
                outputName += "_flipped"
                filters = append(filters, gift.FlipHorizontal())
            }
            g := gift.New(filters...)
            dst := image.NewNRGBA(g.Bounds(src.Bounds()))
            g.Draw(dst, src)

            thumbs[outputName] = dst
        }
    }

    return thumbs
}

func saveThumb(filepath string, img image.Image) {
    fp, err := os.Create(filepath)
    defer fp.Close()

    if err != nil {
        log.Fatal(err)
    }
    err = png.Encode(fp, img)
    if err != nil {
        log.Fatal(err)
    }
}

//=============================================================================

// This is a channel because there are two execution strategies. If you use 
// shuffling with deduplication, everything is loaded into memory first. That's 
// not feasible for some datasets.

var PROCESSORS = runtime.NumCPU() * 2
var fileChan = make(chan string, 4*PROCESSORS)


func validFile(path string, info os.FileInfo) bool {
    baseName := filepath.Base(path)
    return (baseName[0] != '.' && // No hidden files
            !info.IsDir() &&      // Real files
            info.Size() > 0)      // Not just markers
}

func gatherInputs(inputPath string) {
    defer close(fileChan )

    if *shufflePaths {
        var paths []string

        // Gather all paths first.
        filepath.Walk(inputPath, func (path string, info os.FileInfo, err error) error {
            if err == nil && validFile(path, info) {
                paths = append(paths, path)
            }
            return err
        })

        permutation := rand.Perm(len(paths))
        for _, i := range permutation{
            fileChan <- paths[i]
        }

    } else {
        // Write to the channel ASAP.
        filepath.Walk(inputPath, func (path string, info os.FileInfo, err error) error {
            if err == nil && validFile(path, info) {
                fileChan <- path
            }
            return err
        })
    }
}

func outputPath(inputPath string, ensureDir bool) string {
    srcDir, srcName := filepath.Split(inputPath)
    parts := strings.Split(filepath.ToSlash(srcDir), "/")
    dstDir := *outputDir

    if len(parts) > 1 {
        dstDir = filepath.Join(dstDir, filepath.Join(parts[1:]...))
    }

    if ensureDir {
        os.MkdirAll(dstDir, os.ModePerm)
    }

    return filepath.Join(dstDir, srcName)
}

var checksumMutex sync.Mutex

var checksums = make(map[int64]bool)

func checkChecksum(checksum int64) bool {
    checksumMutex.Lock()
    defer checksumMutex.Unlock()

    if _, found := checksums[checksum]; found {
        return false
    }
    checksums[checksum] = true
    return true
}


func processPath(inputFile string) {

    fmt.Println(inputFile)

    img, checksum, err := readImage(inputFile)

    if *deduplicate && !checkChecksum(checksum) {
        fmt.Println("Skipping", inputFile)
        return
    }

    if err != nil{
        log.Fatal(err)
    }

    thumbs := createThumb(img, ANCHORINGS)

    outputFile := outputPath(inputFile, true)
    d, name := filepath.Split(outputFile)
    name = name[:strings.Index(name, ".")]
    for k, v := range thumbs {
        f_p := filepath.Join(d, name + "_" + k + ".png")
        fmt.Println("Saving", f_p)
        saveThumb(f_p, v)
    }

}

func consumer() {
    for inputFile := range fileChan {
        processPath(inputFile)
    }
    defer wg.Done()
}

func receiveInputs() {
    for i := 0; i < PROCESSORS; i++ {
        wg.Add(1)
        go consumer()
    }
}

//=============================================================================

func main() {
    rand.Seed(time.Now().UTC().UnixNano())
    flag.Parse()

    if *flipVertical {
        flipOps = append(flipOps, true)
    }

    gatherInputs(*inputDir)
    receiveInputs()

    wg.Wait()
    fmt.Println("Done")
}
