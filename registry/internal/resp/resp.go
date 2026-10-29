// Package resp implements the subset of Redis RESP2 used by the registry.
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

func WriteCommand(writer io.Writer, arguments ...string) error {
	if _, err := fmt.Fprintf(writer, "*%d\r\n", len(arguments)); err != nil {
		return err
	}
	for _, argument := range arguments {
		if _, err := fmt.Fprintf(writer, "$%d\r\n%s\r\n", len(argument), argument); err != nil {
			return err
		}
	}
	return nil
}

func ReadResponse(reader *bufio.Reader) (any, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	switch prefix {
	case '+':
		return readLine(reader)
	case '-':
		message, lineErr := readLine(reader)
		if lineErr != nil {
			return nil, lineErr
		}
		return nil, errors.New(message)
	case ':':
		line, lineErr := readLine(reader)
		if lineErr != nil {
			return nil, lineErr
		}
		return strconv.ParseInt(line, 10, 64)
	case '$':
		return readBulkString(reader)
	case '*':
		return readArray(reader)
	default:
		return nil, fmt.Errorf("unsupported Redis response prefix %q", prefix)
	}
}

func readLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 2 || !strings.HasSuffix(line, "\r\n") {
		return "", errors.New("malformed Redis response line")
	}
	return line[:len(line)-2], nil
}

func readBulkString(reader *bufio.Reader) (any, error) {
	line, err := readLine(reader)
	if err != nil {
		return nil, err
	}
	length, err := strconv.Atoi(line)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	if length < -1 {
		return nil, errors.New("invalid Redis bulk string length")
	}
	data := make([]byte, length+2)
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, err
	}
	if string(data[length:]) != "\r\n" {
		return nil, errors.New("malformed Redis bulk string")
	}
	return string(data[:length]), nil
}

func readArray(reader *bufio.Reader) (any, error) {
	line, err := readLine(reader)
	if err != nil {
		return nil, err
	}
	length, err := strconv.Atoi(line)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	if length < -1 {
		return nil, errors.New("invalid Redis array length")
	}
	values := make([]any, length)
	for index := range values {
		values[index], err = ReadResponse(reader)
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}
