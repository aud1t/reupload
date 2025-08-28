# reupload
Yandex Adnvanced Code Task

# Копирование таблицы `profiles` между серверами PostgreSQL

## Условие задачи

Есть два сервера PostgreSQL:  
- **PROD** — OLTP сервер,  
- **STATS** — сервер для долгих аналитических запросов.  

На текущем сервере в базе `prod` есть большая (10Tb) таблица вида:

```sql
CREATE TABLE profiles(
    id   BIGSERIAL, 
    data JSONB
);
```

В таблице могут быть "дырки", т.е. некоторые `id` могут быть пропущены.
Для работы с базами данных предполагается использовать следующий интерфейс:

```
type Row []interface{}

type Database interface {
    io.Closer

    GetMaxID(ctx context.Context) (uint64, error)
    LoadRows(ctx context.Context, minID, maxID uint64) ([]Row, error) // [minID, maxID)
    SaveRows(ctx context.Context, rows []Row) error
}

func Connect(ctx context.Context, dbname string) (Database, error)
```

Реализация интерфейса `Database` умеет переустанавливать подключения, вызов `SaveRows` идемпотентен.
Нужно написать функцию для копирования таблицы profiles с `PROD` на `STATS`
```
func CopyTable(fromName string, toName string, full bool) error
```


Если передана опция `full=false`, то программа должна продолжить переливку данных с места прошлой ошибки. Если `full=true` - то должна перелить все данные.

Базовый уровень:
последовательная переливка данных небольшими частями
реализовать восстановление после сбоев (опция `full`)

Продвинутый уровень:
переливка данных в несколько потоков
retry при ошибках, отделение временных (`transient`) от постоянных ошибок

Дополнительная информация:
при необходимости вы можете расширить интерфейс, добавив свои методы
при необходимости вы можете использовать пакет `database/sql` напрямую
