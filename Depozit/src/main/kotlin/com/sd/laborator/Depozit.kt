package com.sd.laborator

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.messaging.Processor
import org.springframework.integration.annotation.Transformer
import java.io.File

@EnableBinding(Processor::class)
@SpringBootApplication
class DepozitMicroservice {
    companion object {
            // Luăm dinamic calea către folderul "Home" al utilizatorului curent
            private val CALE_HOME = System.getProperty("user.home")

            // Fișierele se vor salva direct în folderul Home, la fel pentru toate microserviciile!
            private val FISIER_COMENZI = "$CALE_HOME/comenzi_baza_date.txt"
            private val FISIER_CLIENTI = "$CALE_HOME/clienti_baza_date.txt"
            private val FISIER_FACTURI = "$CALE_HOME/facturi_baza_date.txt"
            private val FISIER_PRODUSE = "$CALE_HOME/produse.txt"
        /**
         * REZOLVARE TODO 1 - Modelare stoc depozit.
         * Folosim un MutableMap (reprezentat dinamic) adaptat la noile produse tech.
         */
        val stocProduse: MutableMap<String, Int> = mutableMapOf(
            "Placa video RTX 5080" to 1150,
            "Procesor AMD Ryzen 9" to 3000,
            "Monitor Gaming OLED" to 1000,
            "Tastatura Mecanica RGB" to 5000,
            "Casti Wireless Pro" to 400,
            "Sursa Corsair 1000W" to 2500
        )
    }

    /**
     * REZOLVARE TODO 2 - Verificare stoc real produs
     */
    private fun verificareStoc(produs: String, cantitate: Int): Boolean {
        val stocDisponibil = stocProduse[produs] ?: 0
        return stocDisponibil >= cantitate
    }

    /**
     * REZOLVARE TODO 3 - Retragere produs de pe stoc in cantitatea specificata
     */
    private fun pregatireColet(produs: String, cantitate: Int): String {
        val stocCurent = stocProduse[produs] ?: 0
        stocProduse[produs] = stocCurent - cantitate
        println("[DEPOZIT] Produsul \"$produs\" in cantitate de $cantitate buc. este pregatit de livrare.")
        println("[DEPOZIT] Stoc ramas pentru \"$produs\": ${stocProduse[produs]}")
        return "APPROVED"
    }

    private fun actualizeazaStatusInFisier(idComanda: String, noulStatus: String) {
        val fisier = File(FISIER_COMENZI)
        if (!fisier.exists()) return

        val liniiModificate = fisier.readLines().map { linie ->
            val parti = linie.split("|")
            if (parti[0] == idComanda) {
                "${parti[0]}|${parti[1]}|${parti[2]}|${parti[3]}|$noulStatus"
            } else {
                linie
            }
        }
        fisier.writeText(liniiModificate.joinToString("\n") + "\n")
    }

    private fun acceptareComanda(identificator: Int, produs: String, cantitate: Int): String {
        println("[DEPOZIT] Comanda cu identificatorul #$identificator a fost acceptata!")
        pregatireColet(produs, cantitate)
        actualizeazaStatusInFisier(identificator.toString(), "APPROVED")
        return identificator.toString() // Intoarcem doar ID-ul comenzii aprobat
    }

    private fun respingereComanda(identificator: Int): String {
        println("[DEPOZIT] Comanda cu identificatorul #$identificator a fost respinsa! Stoc insuficient.")
        actualizeazaStatusInFisier(identificator.toString(), "REJECTED")
        return "RESPINSA_$identificator" // Trimitem un marker de respingere
    }

    @Transformer(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
    fun procesareComanda(idComandaInbound: String?): String {
        if (idComandaInbound.isNullOrBlank()) return ""

        val idComanda = idComandaInbound.trim()
        println("\n[DEPOZIT] Procesez comanda cu identificatorul #$idComanda...")

        // Deschidem "baza de date" de comenzi ca sa aflam ce s-a cerut
        val fisierComenzi = File(FISIER_COMENZI)
        if (!fisierComenzi.exists()) {
            println("[DEPOZIT] Eroare: $FISIER_COMENZI nu exista.")
            return "RESPINSA_$idComanda"
        }

        val linieComanda = fisierComenzi.readLines().find { it.startsWith(idComanda) }
        if (linieComanda == null) {
            println("[DEPOZIT] Comanda #$idComanda nu a fost gasita in fisier.")
            return "RESPINSA_$idComanda"
        }

        // Parsam linia: idComanda|idClient|produs|cantitate|status
        val parti = linieComanda.split("|")
        val produsSolicitat = parti[2]
        val cantitateSolicitata = parti[3].toInt()

        println("[DEPOZIT] S-a cerut: $produsSolicitat in cantitate de $cantitateSolicitata buc.")

        // Rulam logica folosind functiile cerute de profesor
        val rezultatPipeline: String = if (verificareStoc(produsSolicitat, cantitateSolicitata)) {
            acceptareComanda(idComanda.toInt(), produsSolicitat, cantitateSolicitata)
        } else {
            respingereComanda(idComanda.toInt())
        }

        /**
         * REZOLVARE TODO 4 - In loc sa se trimita mesajul cu toate datele,
         * se trimite doar ID-ul comenzii (sau markerul RESPINSA_ID)
         */
        println("[DEPOZIT] Se trimite catre Facturare rezultatul: $rezultatPipeline")
        return rezultatPipeline
    }
}

fun main(args: Array<String>) {
    runApplication<DepozitMicroservice>(*args)
}