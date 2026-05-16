package model

// SignedEnvelope - подписанный конверт для защиты данных при передаче.
type SignedEnvelope struct {
	Payload   []byte // Сериализованные данные (например, PeerRecord или сообщение)
	Signature []byte // Подпись (по ГОСТ)
	PublicKey []byte // Публичный ключ для проверки подписи
}
