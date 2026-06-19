package com.sd.laborator

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.cloud.stream.messaging.Sink
import java.io.File

@EnableBinding(Sink::class)
@SpringBootApplication
class LivrareMicroservice {

    companion object {
        // Luăm dinamic calea către folderul "Home" al utilizatorului curent
        private val CALE_HOME = System.getProperty("user.home")

        // Fișierele se vor salva direct în folderul Home, la fel pentru toate microserviciile!
        private val FISIER_COMENZI = "$CALE_HOME/comenzi_baza_date.txt"
        private val FISIER_CLIENTI = "$CALE_HOME/clienti_baza_date.txt"
        private val FISIER_FACTURI = "$CALE_HOME/facturi_baza_date.txt"
        private val FISIER_PRODUSE = "$CALE_HOME/produse.txt"
    }

    @StreamListener(Sink.INPUT)
    fun expediereComanda(mesajInbound: String) {
        val mesaj = mesajInbound.trim()

        // Cazul 1: Comanda a fost anulată pe parcurs (lipsă stoc sau eroare facturare)
        if (mesaj.startsWith("ANULATA_")) {
            val idComandaAnulata = mesaj.replace("ANULATA_", "")
            println("\n[LIVRARE] Comanda #$idComandaAnulata a fost ANULATĂ în pipeline. Nu se expediază nimic.")
            return
        }

        // Cazul 2: Comanda este aprobată și facturată
        if (mesaj.startsWith("LIVREAZA_")) {
            val idComanda = mesaj.replace("LIVREAZA_", "")
            println("\n[LIVRARE] Se pregătește expediția pentru comanda aprobată: #$idComanda...")

            // 1. Căutăm comanda în baza de date ca să aflăm ID-ul clientului și produsul
            val fisierComenzi = File(FISIER_COMENZI)
            if (!fisierComenzi.exists()) {
                println("[LIVRARE] Eroare criticală: Fișierul de comenzi nu există.")
                return
            }

            val linieComanda = fisierComenzi.readLines().find { it.startsWith(idComanda) }
            if (linieComanda == null) {
                println("[LIVRARE] Comanda #$idComanda nu a fost găsită pe disc.")
                return
            }

            // idComanda|idClient|produs|cantitate|status
            val partiComanda = linieComanda.split("|")
            val idClient = partiComanda[1]
            val produs = partiComanda[2]
            val cantitate = partiComanda[3]

            // 2. Reconstituim datele personale DOAR în acest punct final (GDPR Compliant)
            // Căutăm clientul în tabela/fișierul de clienți pentru a-i afla numele și adresa
            val fisierClienti = File(FISIER_CLIENTI)
            if (!fisierClienti.exists()) {
                println("[LIVRARE] Eroare: Registrul de clienți ($FISIER_CLIENTI) lipsește.")
                return
            }

            val linieClient = fisierClienti.readLines().find { it.startsWith(idClient) }
            if (linieClient == null) {
                println("[LIVRARE] Clientul cu ID-ul $idClient nu a fost găsit în baza de date.")
                return
            }

            // idClient|Nume|Adresă
            val partiClient = linieClient.split("|")
            val numeClient = partiClient[1]
            val adresaLivrare = partiClient[2]

            // 3. Tipărim avizul de expediție final (Aici se vede magria pipeline-ului decuplat)
            println("=================== AVIZ DE EXPEDIȚIE ===================")
            println("Destinatar: $numeClient")
            println("Adresă Livrare: $adresaLivrare")
            println("Conținut Colet: $cantitate x [ $produs ]")
            println("Status Livrare: EXPEDIAT CĂTRE CURIER (Comanda #$idComanda)")
            println("=========================================================")
        }
    }
}

fun main(args: Array<String>) {
    runApplication<LivrareMicroservice>(*args)
}