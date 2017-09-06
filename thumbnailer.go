package main

import (
    "bytes"
    "errors"
    "flag"
    "fmt"
    "gopkg.in/cheggaaa/pb.v1"
    "github.com/disintegration/gift"
    "hash/crc32"
    "image"
    "image/png"
    "io"
    "log"
    "math/rand"
    "os"
    "path/filepath"
    "runtime"
    "strings"
    "strconv"
    "sync"
    "time"
    _ "image/gif"
    _ "image/jpeg"
)

//=============================================================================

var inputDir     = flag.String("i", "image_packs", "input directory")
var outputDir    = flag.String("o", "image_thumbs", "output directory")
var deduplicate  = flag.Bool("n", true, "skip duplicates")
var shufflePaths = flag.Bool("s", true, "shuffle image paths")
var flipVertical = flag.Bool("f", true, "flip vertical")
var verbose      = flag.Bool("v", true, "verbose output")

// This isn't a flag. But, it's populated based on flipVertical.
var flipOps      = []bool{false}

type dim_t [2]int

func (p *dim_t) String() string {
    return fmt.Sprintf("%d,%d", p[0], p[1])
}

func (p *dim_t) Set(raw string) error {
    parts := strings.Split(raw, ",")
    if len(parts) != 2 {
        return errors.New("Dimensions argument expected string like `X,Y`")
    }

    for i, s := range parts {
        v, err := strconv.ParseInt(s, 10, 32)
        if err != nil {
            return fmt.Errorf("%q not an integer in %q", v, raw)
        }
        p[i] = int(v)
    }

    return nil
}

// Default is the receptor field for VGG16.
var thumbDim dim_t = dim_t{224, 224}

func init() {
    flag.Var(&thumbDim, "d", "Thumbnail Dimensions")
}

//=============================================================================

// XXX: TODO: Allow for CLI anchor spec.
var ANCHORINGS = map[string]gift.Anchor{
    "left": gift.LeftAnchor,
    "right": gift.RightAnchor,
    "center": gift.CenterAnchor,
}

//=============================================================================

var wg sync.WaitGroup

func readImage(path string) (img image.Image, checksum int64, err error) {
    fp, err := os.Open(path)
    defer fp.Close()

    if err != nil {
        return nil, -1, err
    }

    buf := bytes.NewBuffer(nil)
    io.Copy(buf, fp)
    checksum = int64(crc32.ChecksumIEEE(buf.Bytes()))

    img, _, err = image.Decode(buf)
    if err != nil {
        return nil, -1, err
    }

    return img, int64(checksum), nil
}


func calcResizeBounds(src image.Image) (int, int) {
    bounds := src.Bounds()

    x, y := bounds.Max.X, bounds.Max.Y

    if x < y {
        s := thumbDim[0] / x
        return thumbDim[0], y * s
    } else {
        s := thumbDim[1] / y
        return x * s, thumbDim[1]
    }
}

func subImage(src image.Image) image.Image {
    x, y := calcResizeBounds(src)

    g := gift.New(gift.Resize(x, y, gift.LanczosResampling))
    dst := image.NewNRGBA(g.Bounds(src.Bounds()))
    g.Draw(dst, src)

    return dst
}


var progressBar *pb.ProgressBar = nil

func createThumbs(src image.Image, anchors map[string]gift.Anchor) map[string]image.Image {
    thumbs := make(map[string]image.Image)

    src = subImage(src)

    for k, anchor := range anchors {
        for _, flipped := range flipOps {
            outputName := k

            filters := []gift.Filter{gift.CropToSize(thumbDim[0], thumbDim[1], anchor)}
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

var nProcessors = runtime.NumCPU() * 2

var filePaths = make(chan string, 4*nProcessors)

func isImageFile(path string, info os.FileInfo) bool {
    baseName := filepath.Base(path)
    return (baseName[0] != '.' && // No hidden files
            !info.IsDir() &&      // Real files
            info.Size() > 0)      // Not just markers
}

func produceInputs(inputPath string) {

    if *shufflePaths {
        var paths []string

        // Gather all paths first.
        filepath.Walk(inputPath, func (path string, info os.FileInfo, err error) error {
            if err == nil && isImageFile(path, info) {
                paths = append(paths, path)
            }
            return err
        })

        // Walk paths shuffled.
        // Why? If you visit sequentially and use deplification, 
        // files that are lexicographically earlier are less likely 
        // to be deleted. It unbalances classes in a nonsensical way.
        wg.Add(1)

        go func() {
            defer func() { close(filePaths); defer wg.Done() }()

            for _, i := range rand.Perm(len(paths)) {
                filePaths <- paths[i]
            }
        }()

        if !*verbose {
            progressBar = pb.StartNew(len(paths))
        }

    } else {
        wg.Add(1)
        go func() {
            defer func() { close(filePaths); defer wg.Done() }()
            // Write to the channel ASAP.
            filepath.Walk(inputPath, func (path string, info os.FileInfo, err error) error {
                if err == nil && isImageFile(path, info) {
                    filePaths <- path
                }
                return err
            })
        }()
    }
}

func outputPath(inputPath string, ensureDir bool) (string, error) {
    srcDir, srcName := filepath.Split(inputPath)
    parts := strings.Split(filepath.ToSlash(srcDir), "/")
    dstDir := *outputDir

    if len(parts) > 1 {
        dstDir = filepath.Join(dstDir, filepath.Join(parts[1:]...))
    } else {
        return "", fmt.Errorf("Can't split %s into parts", inputPath)
    }

    if ensureDir {
        os.MkdirAll(dstDir, os.ModePerm)
    }

    return filepath.Join(dstDir, srcName), nil
}

var checksumMutex sync.Mutex

var checksums = make(map[int64]bool)

func checkChecksum(checksum int64) bool {
    // This should be better than a RWLock for most cases.
    // Usually, you have only a few dupes.
    checksumMutex.Lock()
    defer checksumMutex.Unlock()

    if _, found := checksums[checksum]; found {
        return false
    }
    checksums[checksum] = true
    return true
}


func processPath(inputFile string) {
    if *verbose {
        fmt.Println(inputFile)
    } 
    img, checksum, err := readImage(inputFile)

    if *deduplicate && false && !checkChecksum(checksum) {
        if *verbose {
            fmt.Println("Skipping", inputFile)
        }
        return
    }

    if err != nil{
        log.Fatal(err)
    }

    thumbs := createThumbs(img, ANCHORINGS)

    outputFile, err := outputPath(inputFile, true)
    if err != nil {
        return // Just skip processing
    }

    d, name := filepath.Split(outputFile)
    if j := strings.Index(name, "."); j != -1 {
        name = name[:j]
    }
    for k, v := range thumbs {
        f_p := filepath.Join(d, name + "_" + k + ".png")
        if *verbose {
            fmt.Println("Saving", f_p)
        }
        saveThumb(f_p, v)
    }
}

func consumer() {
    defer wg.Done()
    for inputFile := range filePaths {
        processPath(inputFile)

        if (*shufflePaths && !*verbose) {
            progressBar.Increment()
        }
    }
}

func receiveInputs() {
    for i := 0; i < nProcessors; i++ {
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

    produceInputs(*inputDir)
    receiveInputs()

    wg.Wait()
    fmt.Println("Done")
}
