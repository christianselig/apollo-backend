module github.com/christianselig/apollo-backend

go 1.18

require (
	github.com/DataDog/datadog-go v4.8.3+incompatible
	github.com/adjust/rmq/v5 v5.1.1
	github.com/bugsnag/bugsnag-go/v2 v2.2.0
	github.com/dustin/go-humanize v1.0.1
	github.com/go-co-op/gocron v1.19.0
	github.com/go-ozzo/ozzo-validation/v4 v4.3.0
	github.com/go-redis/redis/extra/redisotel/v8 v8.11.5
	github.com/go-redis/redis/v8 v8.11.5
	github.com/gofrs/uuid v4.4.0+incompatible
	github.com/gorilla/mux v1.8.0
	github.com/heroku/x v0.0.55
	github.com/honeycombio/honeycomb-opentelemetry-go v0.5.0
	github.com/honeycombio/opentelemetry-go-contrib/launcher v0.0.0-20221031150637-a3c60ed98d54
	github.com/jackc/pgx/v5 v5.3.1
	github.com/joho/godotenv v1.5.1
	github.com/sideshow/apns2 v0.23.0
	github.com/smtp2go-oss/smtp2go-go v1.0.2
	github.com/spf13/cobra v1.6.1
	github.com/stretchr/testify v1.8.3
	github.com/valyala/fastjson v1.6.4
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.42.0
	go.opentelemetry.io/otel v1.16.0
	go.opentelemetry.io/otel/trace v1.16.0
	go.uber.org/zap v1.24.0
)

require (
	github.com/Microsoft/go-winio v0.6.0 // indirect
	github.com/bugsnag/panicwrap v1.3.4 // indirect
	github.com/cenkalti/backoff/v4 v4.2.0 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/felixge/httpsnoop v1.0.3 // indirect
	github.com/go-logr/logr v1.2.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-redis/redis/extra/rediscmd/v8 v8.11.5 // indirect
	github.com/golang-jwt/jwt/v4 v4.5.0 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.15.2 // indirect
	github.com/honeycombio/otel-launcher-go v0.3.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.0 // indirect
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0 // indirect
	github.com/lufia/plan9stats v0.0.0-20230110061619-bbe2e5e100de // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20221212215047-62379fc7944b // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	github.com/sethvargo/go-envconfig v0.9.0 // indirect
	github.com/shirou/gopsutil/v3 v3.23.2 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/tklauser/go-sysconf v0.3.11 // indirect
	github.com/tklauser/numcpus v0.6.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.opentelemetry.io/contrib/instrumentation/host v0.40.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/runtime v0.40.0 // indirect
	go.opentelemetry.io/contrib/propagators/b3 v1.15.0 // indirect
	go.opentelemetry.io/contrib/propagators/ot v1.15.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/internal/retry v1.14.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric v0.37.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v0.37.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v0.37.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.14.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.14.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.14.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.14.0 // indirect
	go.opentelemetry.io/otel/metric v1.16.0 // indirect
	go.opentelemetry.io/otel/sdk v1.14.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v0.37.0 // indirect
	go.opentelemetry.io/proto/otlp v0.19.0 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/crypto v0.7.0 // indirect
	golang.org/x/mod v0.9.0 // indirect
	golang.org/x/net v0.8.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/sys v0.6.0 // indirect
	golang.org/x/text v0.8.0 // indirect
	golang.org/x/tools v0.7.0 // indirect
	google.golang.org/genproto v0.0.0-20230323212658-478b75c54725 // indirect
	google.golang.org/grpc v1.54.0 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
