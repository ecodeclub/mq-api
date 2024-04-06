APP_PATH:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
SCRIPTS_PATH:=$(APP_PATH)/scripts

.PHONY:	setup
setup:
	@echo "初始化开发环境......"
	@find "$(SCRIPTS_PATH)" -type f -name '*.sh' -exec chmod +x {} \;
	@bash $(SCRIPTS_PATH)/setup/setup.sh
	@make tidy

# 依赖清理
.PHONY: tidy
tidy:
	@go mod tidy

# 代码风格
.PHONY: fmt
fmt:
	@goimports -l -w $$(find . -type f -name '*.go' -not -path "./.idea/*")
	@gofumpt -l -w $$(find . -type f -name '*.go' -not -path "./.idea/*")

# 静态扫描
.PHONY:	lint
lint:
	@golangci-lint run -c $(SCRIPTS_PATH)/lint/.golangci.yaml ./...

# 单元测试
.PHONY:	ut
ut:
	@go test -race -cover -coverprofile=unit.out -failfast -shuffle=on ./...

# 端到端测试
.PHONY: e2e
e2e:
	@make dev_3rd_down
	@make dev_3rd_up
	@go test -tags=e2e -race -cover -coverprofile=e2e.out -failfast -shuffle=on ./internal/e2e/...
	@make dev_3rd_down

# 启动本地研发 docker 依赖
.PHONY: dev_3rd_up
dev_3rd_up:
	@docker compose -p mq-api -f ./scripts/deploy/dev-compose.yaml up -d

.PHONY: dev_3rd_down
dev_3rd_down:
	@docker compose -p mq-api -f ./scripts/deploy/dev-compose.yaml down -v

.PHONY: check
check:
	@echo "\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> 检查阶段 <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n"
	@echo "整理项目依赖中......"
	@make tidy
	@echo "代码风格检查中......"
	@make fmt
	@echo "代码静态扫描中......"
	@make lint