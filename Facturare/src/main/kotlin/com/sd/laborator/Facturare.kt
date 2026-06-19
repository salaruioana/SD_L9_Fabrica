package com.sd.laborator

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.messaging.Processor
import org.springframework.integration.annotation.Transformer
import java.io.File
import kotlin.math.abs
import kotlin.random.Random
import kotlin.random.nextUInt

@EnableBinding(Processor::class)
@SpringBootApplication
class FacturareMicroservice {
    companion object {
        // Luăm dinamic calea către folderul "Home" al utilizatorului curent
        private val CALE_HOME = System.getProperty("user.home")

        // Fișierele se vor salva direct în folderul Home, la fel pentru toate microserviciile!
        private val FISIER_COMENZI = "$CALE_HOME/comenzi_baza_date.txt"
        private val FISIER_CLIENTI = "$CALE_HOME/clienti_baza_date.txt"
        private val FISIER_FACTURI = "$CALE_HOME/facturi_baza_date.txt"
        private val FISIER_PRODUSE = "$CALE_HOME/produse.txt"
    }
    @Transformer(inputChannel = Processor.INPUT, outputChannel = Processor.OUTPUT)
    ///TODO - parametrul ar trebui sa fie doar numarul de inregistrare al comenzii si atat
    fun emitereFactura(idComandainbound: String?): String {
        if ( idComandainbound.isNullOrBlank())
            return ""
        val mesajPrimit = idComandainbound.trim()

        if (mesajPrimit.startsWith("RESPINSA_")) {
            val idComandaRespinsa = mesajPrimit.replace("RESPINSA_", "")
            println("[FACTURARE] Comanda #$idComandaRespinsa a fost respinsă de depozit. NU se emite factură!")
            return "ANULATA_$idComandaRespinsa"
        }

        val idComanda = mesajPrimit
        println("\n[FACTURARE] Se emite factura pentru comanda aprobată: #$idComanda...")

        val fisierComenzi = File(FISIER_COMENZI)
        if (!fisierComenzi.exists()) {
            println("[FACTURARE] Eroare: Fișierul de comenzi nu există.")
            return "ANULATA_$idComanda"
        }
        val linieComanda = fisierComenzi.readLines().find { it.startsWith(idComanda) }
        if (linieComanda == null) {
            println("[FACTURARE] Comanda #$idComanda nu a fost găsită în baza de date pentru facturare.")
            return "ANULATA_$idComanda"
        }

        // Parsăm: idComanda|idClient|produs|cantitate|status
        val parti = linieComanda.split("|")
        val idClient = parti[1]
        val produs = parti[2]
        val cantitate = parti[3].toInt()

        // REZOLVARE TODO: Generare detalii factură și calcul preț simplist
        val nrFactura = abs(Random.nextInt(100000, 999999))
        val pretUnitarFictiv = Random.nextInt(50, 500) // prețuri în RON pentru componente tech
        val totalPlata = cantitate * pretUnitarFictiv

        println("[FACTURARE] Detalii factură emise:")
        println("Factura nr: $nrFactura | Client ID: $idClient | Total: $totalPlata RON")

        // REZOLVARE TODO: Înregistrare factură în baza de date (fișier text)
        val linieFactura = "$nrFactura|$idComanda|$idClient|$totalPlata\n"
        File(FISIER_FACTURI).appendText(linieFactura)
        println("[FACTURARE] Factura nr. $nrFactura a fost salvată în $FISIER_FACTURI.")

        /**
         * REZOLVARE TODO: Păstrăm decuplarea DDD.
         * Trimitem către serviciul de Livrare doar confirmarea și ID-ul comenzii!
         */
        return "LIVREAZA_$idComanda"

    }
}

fun main(args: Array<String>) {
    runApplication<FacturareMicroservice>(*args)
}