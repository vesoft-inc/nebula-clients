package nebula

// func (pool *ConnectionPool) InitPool(addresses []HostAddress, conf conf.PoolConfig, log logger.DefaultLogger) error {
// 	// Process domain to IP
// 	convAddress, err := DomainToIP(addresses)
// 	if err != nil {
// 		return fmt.Errorf("Failed to find IP, error: %s ", err.Error())
// 	}

// 	pool.addresses = convAddress
// 	pool.conf = conf
// 	pool.hostIndex = 0
// 	pool.log = log
// 	// Check input
// 	if len(addresses) == 0 {
// 		return fmt.Errorf("Failed to initialize connection pool: illegal address input")
// 	}
// 	if &conf == nil {
// 		return fmt.Errorf("Failed to initialize connection pool: no configuration")
// 	}

// 	for i := 0; i < pool.conf.MinConnPoolSize; i++ {
// 		// Simple round-robin
// 		newConn := NewConnection(pool.addresses[i%len(addresses)])

// 		// Open connection to host
// 		err := newConn.Open(newConn.SeverAddress, pool.conf)
// 		if err != nil {
// 			return fmt.Errorf("Failed to open connection, error: %s ", err.Error())
// 		}
// 		// Mark connection as in use
// 		pool.idleConnectionQueue.PushBack(newConn)
// 	}
// 	pool.log.Info("Connection pool is initialized successfully")
// 	return nil
// }
