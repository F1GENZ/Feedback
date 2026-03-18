# F1GENZ Feedback Server

## Telegram Webhook Management

### 🔗 Kết nối Webhook
```
https://api-feedback.f1genz.dev/api/telegram-setup
```
Hoặc chỉ định URL cụ thể:
```
https://api-feedback.f1genz.dev/api/telegram-setup?url=https://api-feedback.f1genz.dev/api/telegram-webhook
```

### ❌ Xóa Webhook
Mở trình duyệt hoặc dùng curl:
```
https://api.telegram.org/bot<BOT_TOKEN>/deleteWebhook?drop_pending_updates=true
```
> `drop_pending_updates=true` sẽ xóa hết tin nhắn đang chờ xử lý (tránh ghost messages).

### 📋 Kiểm tra trạng thái Webhook
```
https://api-feedback.f1genz.dev/api/telegram-info
```
Hoặc trực tiếp từ Telegram API:
```
https://api.telegram.org/bot<BOT_TOKEN>/getWebhookInfo
```

### 🔄 Quy trình Reset Webhook (khi bị lỗi)
1. **Xóa webhook cũ:**
   ```
   https://api.telegram.org/bot<BOT_TOKEN>/deleteWebhook?drop_pending_updates=true
   ```
2. **Chờ 5 giây**
3. **Kết nối lại:**
   ```
   https://api-feedback.f1genz.dev/api/telegram-setup
   ```
4. **Kiểm tra:**
   ```
   https://api-feedback.f1genz.dev/api/telegram-info
   ```

---

## Bot Commands

| Lệnh | Mô tả |
|-------|--------|
| `/f <nội dung>` | Tạo feedback mới (auto parse shop, link, tags, host) |
| `//` hoặc `/r` | Xem feedback của mình |
| `// Tên` hoặc `/rTên` | Xem feedback theo host |
| `/rall` | Xem tất cả feedback |
| `/r<shop>` | Xem feedback theo shop |
| Reply `done` | Đánh dấu hoàn thành |
| Reply `done <ghi chú>` | Hoàn thành + comment |
| Reply `del` | Xóa feedback (Stage = Deleted) |
| Reply text/ảnh | Thêm comment |
| `/myid` | Xem Telegram User ID |
| `/groupid` | Xem Group Chat ID |
| `/help` | Xem hướng dẫn |

---

## Deploy

Server tự động deploy qua GitHub Actions khi push lên `main`.

### Manual restart trên VPS:
```bash
pm2 restart feedback
```

### Xem logs:
```bash
pm2 logs feedback --lines 50
```
