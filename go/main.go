package main

import (
  "log"

  "github.com/Liquid-Labs/catalyst-core-api/go/restserv"
  // core resources
  "github.com/Liquid-Labs/catalyst-core-api/go/resources/entities"
  "github.com/Liquid-Labs/catalyst-core-api/go/resources/users"

  "github.com/Liquid-Labs/catalyst-content-api/go/resources/content"
  "github.com/Liquid-Labs/go-api/sqldb"
)

func main() {
  // dependencies
  sqldb.RegisterSetup(entities.SetupDB)
  sqldb.RegisterSetup(users.SetupDB)
  // our DB
  sqldb.RegisterSetup(content.SetupDB)
  sqldb.InitDB()
  // our API
  restserv.RegisterResource(content.InitAPI)
  restserv.Init()
  log.Print("Init done.")
}
