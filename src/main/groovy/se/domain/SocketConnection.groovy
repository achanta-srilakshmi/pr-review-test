package se.domain

import grails.gorm.annotation.Entity

import java.sql.Timestamp

@Entity
class SocketConnection {
    String sessionId
    String identifier
    Timestamp version
    Boolean isActive = true

    static mapping = {
        table 'connections'
        sessionId unique: true
    }
}
