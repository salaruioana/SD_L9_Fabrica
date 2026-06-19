package com.sd.laborator

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.messaging.Processor
import org.springframework.integration.annotation.Transformer
import org.springframework.messaging.support.MessageBuilder
import java.io.File
import java.lang.Math.abs
import java.text.DateFormat
import java.text.SimpleDateFormat
import kotlin.random.Random

@EnableBinding(Processor::class)
@SpringBootApplication
class ComandaMicroservice {
    companion object {
        // Luăm dinamic calea către folderul "Home" al utilizatorului curent
        private val CALE_HOME = System.getProperty("user.home")

        // Fișierele se vor salva direct în folderul Home, la fel pentru toate microserviciile!
        private val FISIER_COMENZI = "$CALE_HOME/comenzi_baza_date.txt"
        private val FISIER_CLIENTI = "$CALE_HOME/clienti_baza_date.txt"
        private val FISIER_FACTURI = "$CALE_HOME/facturi_baza_date.txt"
        private val FISIER_PRODUSE = "$CALE_HOME/produse.txt"
    }
    private fun pregatireComanda(idClient: String, produs: String, cantitate: Int): Int {
        println("Se pregateste comanda $cantitate x \"$produs\"...")

        ///TODO - asignare numar de inregistrare
        val idComanda = abs(Random.nextInt(100000,999999))
        println("[COMANDĂ] Se pregătește comanda #$idComanda: $cantitate x \"$produs\" pentru Clientul $idClient...")

        ///TODO - inregistrare comanda in baza de date
        val linieComanda = "$idComanda|$idClient|$produs|$cantitate|PENDING\n"
        File(FISIER_COMENZI).appendText(linieComanda)
        return idComanda
    }

    @Transformer(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
    fun preluareComanda(comanda: String?): String {
        if (comanda.isNullOrBlank()) return ""
        val parti = comanda.split("|")
        val idClient = parti[0]
        val produsComandat = parti[1]
        val cantitate = parti[2].toInt()

        println("\n[COMANDĂ] Am primit o cerere de comandă nouă de la Sursă:")
        println("ID Client: $idClient | Produs: $produsComandat | Cantitate: $cantitate")

        val idComanda = pregatireComanda(idClient, produsComandat, cantitate.toInt())

        ///TODO - in loc sa se trimita mesajul cu toate datele in continuare, trebuie trimis doar ID-ul comenzii
        println("[COMANDĂ] Se trimite mai departe în pipeline doar ID-ul comenzii: $idComanda")
        return idComanda.toString()
    }
}

fun main(args: Array<String>) {
    runApplication<ComandaMicroservice>(*args)
}