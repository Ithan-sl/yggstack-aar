package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/celestix/gotgproto"
	"github.com/celestix/gotgproto/sessionMaker"
	"github.com/gotd/td/tg"
)

const (
	apiID   = 30271769
	apiHash = "564dc69f53ba4b0826d6818c58de5a60"
)

var client *gotgproto.Client

func main() {
	log.Println("Iniciando o Motor de Streaming em Go...")

	// 1. Inicia o Cliente do Telegram e gerencia a sessão
	var err error
	client, err = gotgproto.NewClient(
		apiID,
		apiHash,
		gotgproto.ClientTypePhone("sessao_golang"),
		&gotgproto.ClientOpts{
			Session: sessionMaker.NewSession("sessao_golang", sessionMaker.SqliteSession),
		},
	)

	if err != nil {
		log.Fatalf("Erro fatal ao iniciar Telegram: %v", err)
	}

	log.Println("[+] Servidor Go Conectado ao Telegram!")

	// 2. Configura as rotas HTTP
	http.HandleFunc("/stream/", streamHandler)

	// 3. Inicia o Servidor na porta 8000
	log.Println("[+] Uvicorn aposentado! Servidor Go escutando na porta 8000")
	if err := http.ListenAndServe("0.0.0.0:8000", nil); err != nil {
		log.Fatalf("Erro no servidor HTTP: %v", err)
	}
}

func streamHandler(w http.ResponseWriter, r *http.Request) {
	// CORS para o Player Vidstack
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Range")

	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 {
		http.Error(w, "URL Invalida", http.StatusBadRequest)
		return
	}

	channelIDStr, messageIDStr := parts[1], parts[2]
	channelID, _ := strconv.ParseInt(channelIDStr, 10, 64)
	msgID, _ := strconv.Atoi(messageIDStr)

	// Contexto base
	ctx := context.Background()

	// Resolve o peer (Canal)
	peer, err := client.Context().Resolve(ctx, channelIDStr)
	if err != nil {
		// Fallback para ID numérico se a string falhar
		peer = &tg.InputPeerChannel{ChannelID: channelID} 
	}

	// Busca a mensagem
	messagesClass, err := client.API().ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
		Channel: peer.(*tg.InputPeerChannel),
		ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: msgID}},
	})

	if err != nil {
		http.Error(w, "Erro ao buscar video no Telegram", http.StatusNotFound)
		return
	}

	var fileLocation tg.InputFileLocationClass
	var fileSize int64

	// Extrai a mídia e o tamanho (simplificado para documentos/vídeos)
	msgs, ok := messagesClass.(*tg.MessagesChannelMessages)
	if !ok || len(msgs.Messages) == 0 {
		http.Error(w, "Video nao encontrado", http.StatusNotFound)
		return
	}

	msg, ok := msgs.Messages[0].(*tg.Message)
	if !ok || msg.Media == nil {
		http.Error(w, "Mensagem nao possui midia", http.StatusNotFound)
		return
	}

	doc, ok := msg.Media.(*tg.MessageMediaDocument).Document.(*tg.Document)
	if !ok {
		http.Error(w, "Midia nao e um documento de video", http.StatusNotFound)
		return
	}

	fileSize = doc.Size
	fileLocation = &tg.InputDocumentFileLocation{
		ID:            doc.ID,
		AccessHash:    doc.AccessHash,
		FileReference: doc.FileReference,
	}

	// Calcula os Ranges solicitados pelo player HTML5
	start, end := int64(0), fileSize-1
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		re := regexp.MustCompile(`bytes=(\d+)-(\d*)`)
		matches := re.FindStringSubmatch(rangeHeader)
		if len(matches) > 1 {
			start, _ = strconv.ParseInt(matches[1], 10, 64)
			if matches[2] != "" {
				end, _ = strconv.ParseInt(matches[2], 10, 64)
			}
		}
	}

	chunkLength := (end - start) + 1

	// Responde com os Headers corretos
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, fileSize))
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatInt(chunkLength, 10))
	w.Header().Set("Content-Type", "video/mp4")

	if rangeHeader != "" {
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	// PRE-FETCHING E STREAMING EXTREMAMENTE RÁPIDO (GOROUTINE + CHANNELS)
	// Isso substitui a Fila do Python. É aqui que a mágica do Go acontece.
	chunkChan := make(chan []byte, 20) // Guarda 20 pedaços na RAM
	errChan := make(chan error, 1)

	go func() {
		defer close(chunkChan)
		offset := start
		limit := int64(512 * 1024) // Puxa 512KB por vez direto do Telegram

		for offset <= end {
			reqSize := limit
			if offset+limit > end+1 {
				reqSize = (end + 1) - offset
			}

			req := &tg.UploadGetFileRequest{
				Offset:   offset,
				Limit:    int(reqSize),
				Location: fileLocation,
			}

			res, err := client.API().UploadGetFile(ctx, req)
			if err != nil {
				errChan <- err
				return
			}

			uploadFile, ok := res.(*tg.UploadFile)
			if !ok || len(uploadFile.Bytes) == 0 {
				break
			}

			chunkChan <- uploadFile.Bytes
			offset += int64(len(uploadFile.Bytes))
		}
	}()

	// Joga os dados do Canal Go diretamente para a tela do usuário
	for chunk := range chunkChan {
		_, err := w.Write(chunk)
		if err != nil {
			break // Se o usuário fechar a aba, cancela a entrega suavemente
		}
		w.(http.Flusher).Flush() // Empurra os dados instantaneamente sem segurar na rede
	}
}
