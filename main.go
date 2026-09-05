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

	var err error
	// Inicia o Cliente e cria a sessão SQLite para não precisar logar de novo
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

	http.HandleFunc("/stream/", streamHandler)
	log.Println("[+] Uvicorn aposentado! Servidor Go escutando na porta 8000")
	
	if err := http.ListenAndServe("0.0.0.0:8000", nil); err != nil {
		log.Fatalf("Erro no servidor HTTP: %v", err)
	}
}

func streamHandler(w http.ResponseWriter, r *http.Request) {
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
	if channelID < -1000000000000 {
		channelID = (channelID * -1) - 1000000000000
	} else if channelID < 0 {
		channelID = channelID * -1
	}

	msgID, _ := strconv.Atoi(messageIDStr)
	ctx := context.Background()

	peer := &tg.InputChannel{
		ChannelID:  channelID,
		AccessHash: 0,
	}

	messagesClass, err := client.API().ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
		Channel: peer,
		ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: msgID}},
	})

	if err != nil {
		dialogs, errDialogs := client.API().MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
			OffsetPeer: &tg.InputPeerEmpty{},
			Limit:      100,
		})
		
		if errDialogs == nil {
			if dlgs, ok := dialogs.(*tg.MessagesDialogsSlice); ok {
				for _, chat := range dlgs.Chats {
					if c, ok := chat.(*tg.Channel); ok && c.ID == channelID {
						peer.AccessHash = c.AccessHash
						break
					}
				}
			}
		}
		
		messagesClass, err = client.API().ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
			Channel: peer,
			ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: msgID}},
		})
		if err != nil {
			http.Error(w, "Erro ao buscar video no Telegram", http.StatusNotFound)
			return
		}
	}

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
		http.Error(w, "Midia nao e um documento", http.StatusNotFound)
		return
	}

	fileLocation := &tg.InputDocumentFileLocation{
		ID:            doc.ID,
		AccessHash:    doc.AccessHash,
		FileReference: doc.FileReference,
	}

	fileSize := doc.Size
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

	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, fileSize))
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatInt(chunkLength, 10))
	w.Header().Set("Content-Type", "video/mp4")

	if rangeHeader != "" {
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	// CANAL GO: A Magica da Velocidade Extrema
	chunkChan := make(chan []byte, 20)

	go func() {
		defer close(chunkChan)
		offset := start
		limit := int64(512 * 1024)

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

	for chunk := range chunkChan {
		_, err := w.Write(chunk)
		if err != nil {
			break
		}
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}
}
