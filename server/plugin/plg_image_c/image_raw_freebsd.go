package plg_image_c

// #include "image_raw.h"
// #cgo LDFLAGS: -L /usr/local/lib -L /usr/lib -L /lib -lyuv -ljpeg -lraw -fopenmp -lc++ -llcms2 -lm
// #cgo CFLAGS: -I /usr/local/include
import "C"

func raw(input uintptr, output uintptr, size int) {
	C.raw_to_jpeg(C.int(input), C.int(output), C.int(size))
	return
}
