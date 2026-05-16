package framing

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	// MaxFrameSize - максимальный размер одного сообщения ( 8 Мегабайт). Это критически важная константа для защиты от Out-Of-Memory атак.
	// Без нее злоумышленник мог бы прислать длину 4 ГБ (0xFFFFFFFF) и узел упал бы, попытавшись выделить столько оперативной памяти.
	MaxFrameSize = 8 * 1024 * 1024
)

var (
	// ErrFrameTooLarge возвращается, если размер входящего или исходящего сообщения превышает лимит.
	ErrFrameTooLarge = errors.New("frame size exceeds maximum limit")
)

// LengthPrefixFramer оборачивает любое TCP-соединение (io.ReadWriter)
// и обеспечивает чтение/запись цельных фреймов.
type LengthPrefixFramer struct {
	rw io.ReadWriter
}

// NewLengthPrefixFramer создает новый фреймер для соединения.
func NewLengthPrefixFramer(rw io.ReadWriter) *LengthPrefixFramer {
	return &LengthPrefixFramer{
		rw: rw,
	}
}

// WriteFrame записывает сообщение в сетевой поток.
func (f *LengthPrefixFramer) WriteFrame(data []byte) error {
	size := len(data)
	// 1. Проверяем, не слишком ли большое сообщение мы пытаемся отправить
	if size > MaxFrameSize {
		return ErrFrameTooLarge
	}
	// 2. Создаем буфер из 4 байт для хранения длины сообщения.
	sizeBuf := make([]byte, 4)

	// Используем BigEndian (сетевой порядок байт) для перевода числа в байты
	binary.BigEndian.PutUint32(sizeBuf, uint32(size))
	// 3. Отправляем 4 байта длины в сеть
	if _, err := f.rw.Write(sizeBuf); err != nil {
		return err
	}
	// 4. Отправляем само сообщение (payload)
	// Если data пустая (size == 0), Write ничего не сделает, это нормально.
	if size > 0 {
		if _, err := f.rw.Write(data); err != nil {
			return err
		}
	}
	return nil
}

// ReadFrame читает одно целое сообщение из сетевого потока.
func (f *LengthPrefixFramer) ReadFrame() ([]byte, error) {
	// 1. Сначала читаем ровно 4 байта (префикс длины)
	sizeBuf := make([]byte, 4)
	// ВАЖНО: Используем io.ReadFull, а не f.rw.Read!
	// TCP может отдать данные кусками (сначала 1 байт, потом еще 3). io.ReadFull гарантированно дождется получения всех 4 байт или вернет ошибку.
	if _, err := io.ReadFull(f.rw, sizeBuf); err != nil {
		return nil, err
	}
	// 2. Декодируем длину из байтов обратно в число
	size := binary.BigEndian.Uint32(sizeBuf)
	// 3. Защита от OOM: проверяем размер ПЕРЕД выделением памяти
	if size > MaxFrameSize {
		return nil, ErrFrameTooLarge
	}
	// Если пришло пустое сообщение (ping/keepalive), просто возвращаем пустой срез
	if size == 0 {
		return []byte{}, nil
	}
	// 4. Выделяем память под сообщение ( мы точно знаем, что размер безопасен)
	data := make([]byte, size)
	// 5. Читаем само сообщение.
	// Снова используем io.ReadFull, чтобы дождаться всего тела сообщения из TCP-потока.
	if _, err := io.ReadFull(f.rw, data); err != nil {
		return nil, err
	}

	return data, nil
}
