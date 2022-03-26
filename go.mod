module github.com/christianselig/apollo-backend

// +heroku goVersion go1.16
go 1.16

require (
	github.com/DataDog/datadog-go v4.8.3+incompatible
	github.com/Microsoft/go-winio v0.5.0 // indirect
	github.com/adjust/rmq/v4 v4.0.1
	github.com/dgrijalva/jwt-go v3.2.0+incompatible // indirect
	github.com/dustin/go-humanize v1.0.0
	github.com/go-co-op/gocron v1.13.0
	github.com/go-ozzo/ozzo-validation/v4 v4.3.0
	github.com/go-redis/redis/v8 v8.11.5
	github.com/go-redis/redismock/v8 v8.0.6
	github.com/gorilla/mux v1.8.0
	github.com/heroku/x v0.0.50
	github.com/jackc/pgx/v4 v4.15.0
	github.com/joho/godotenv v1.4.0
	github.com/sideshow/apns2 v0.20.0
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/cobra v1.2.1
	github.com/stretchr/testify v1.7.1
	github.com/valyala/fastjson v1.6.3
)
