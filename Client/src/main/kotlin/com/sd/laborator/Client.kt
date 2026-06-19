package com.sd.laborator

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.messaging.Source
import org.springframework.context.annotation.Bean
import org.springframework.integration.annotation.InboundChannelAdapter
import org.springframework.integration.annotation.Poller
import org.springframework.messaging.Message
import org.springframework.messaging.support.MessageBuilder
import java.io.File
import kotlin.random.Random

@EnableBinding(Source::class)
@SpringBootApplication
class ClientMicroservice {
    companion object {
        private val CALE_HOME = System.getProperty("user.home")

        // Fișierele se vor salva direct în folderul Home, la fel pentru toate microserviciile!
        private val FISIER_COMENZI = "$CALE_HOME/comenzi_baza_date.txt"
        private val FISIER_CLIENTI = "$CALE_HOME/clienti_baza_date.txt"
        private val FISIER_FACTURI = "$CALE_HOME/facturi_baza_date.txt"
        private val FISIER_PRODUSE = "$CALE_HOME/produse.txt"

        // Preluare dinamică a produselor din fișier
        val listaProduse: List<String>
            get() {
                val fisier = File(FISIER_PRODUSE)
                return if (fisier.exists()) {
                    fisier.readLines().filter { it.isNotBlank() }
                } else {
                    println("[WARN] Fișierul $FISIER_PRODUSE nu există! Folosesc lista de backup.")
                    arrayListOf("Placa video RTX 5080", "Procesor AMD Ryzen 9", "Monitor Gaming OLED")
                }
            }

        /**
         * REZOLVARE TODO - Citirea dinamică a clienților existenți din "baza de date"
         * Extragere doar a ID-urilor pentru a simula comenzi valide
         */
        val listaIdClienti: List<String>
            get() {
                val fisier = File(FISIER_CLIENTI)
                return if (fisier.exists()) {
                    // Citim liniile și luăm doar prima parte (ID-ul) din formatul "ID|Nume|Adresă"
                    fisier.readLines()
                        .filter { it.isNotBlank() }
                        .map { it.split("|")[0] }
                } else {
                    println("[WARN] Fișierul $FISIER_CLIENTI nu există! Generez unul de test.")
                    // Dacă fișierul nu există, îl creăm cu câțiva clienți de test
                    fisier.writeText("C100|Popescu Ion|Strada Lalelelor nr. 5, Iasi\nC200|Ionescu Maria|Bulevardul Unirii nr. 12, Bucuresti\n")
                    arrayListOf("C100", "C200")
                }
            }
    }

    @Bean
    @InboundChannelAdapter(value = Source.OUTPUT, poller = [Poller(fixedDelay = "10000", maxMessagesPerPoll = "1")])
    fun comandaProdus(): () -> Message<String> {
        return {
            // Alegem un produs și o cantitate la întâmplare
            val produsComandat = listaProduse[(0 until listaProduse.size).shuffled()[0]]
            val cantitate: Int = Random.nextInt(1, 5) // cantități mai mici și realiste pentru tech

            // Alegem un ID de client existent din baza de date locală
            val idClientMecanic = listaIdClienti[(0 until listaIdClienti.size).shuffled()[0]]

            println("\n[SURSĂ] Clientul cu ID-ul $idClientMecanic plasează o comandă nouă...")
            println("[SURSĂ] Produs cerut: $produsComandat | Cantitate: $cantitate buc.")

            // REDUCERE CUPLARE + GDPR: Trimitem pe rețea doar ID-ul clientului, NU numele sau adresa!
            val mesaj = "$idClientMecanic|$produsComandat|$cantitate"

            // Corecție: variabila trimisă trebuie să fie cea extrasă mai sus
            val payloadSecurizat = "$idClientMecanic|$produsComandat|$cantitate"
            MessageBuilder.withPayload(payloadSecurizat).build()
        }
    }
}

fun main(args: Array<String>) {
    runApplication<ClientMicroservice>(*args)
}