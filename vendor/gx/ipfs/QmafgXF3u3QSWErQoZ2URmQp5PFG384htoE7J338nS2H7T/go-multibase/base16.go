package multibase

func hexEncodeToStringUpper(src []byte) string {
	dst := make([]byte, len(src)*2)
	hexEncodeUpper(dst, src)
	return string(dst)
}

var hextableUpper = [16]byte{
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
	'A', 'B', 'C', 'D', 'E', 'F',
}

func hexEncodeUpper(dst, src []byte) int {
	for i, v := range src {
		dst[i*2] = hextableUpper[v>>4]
		dst[i*2+1] = hextableUpper[v&0x0f]
	}

	return len(src) * 2
}
