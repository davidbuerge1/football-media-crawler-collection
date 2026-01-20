# Wie der Crawler funktioniert (einfach erklaert)

Ein Crawler durchsucht eine Website automatisch nach passenden Seiten.
In diesen Projekten wird die Suche ueber **Sitemaps** gemacht.

## Was ist eine Sitemap?
Eine Sitemap ist eine Datei (meist XML), die eine Liste von Seiten einer Website enthaelt.
Sie wird von der Website bereitgestellt, damit Suchmaschinen und Programme alle Inhalte finden.

Beispiel für eine sitemap: [Link](https://www.srf.ch/sitemaps/aron/articles/2025_12.xml)


## Was macht der Crawler mit der Sitemap?
- Er laedt die Sitemap(s) der jeweiligen Website.
- Er liest alle Links aus und filtert nur Fussball-Seiten.
- Er speichert alle gefundenen Links in eine URL-CSV.
- Er zaehlt pro Jahr, wie viele Links zu Frauen- und Herrenfussball gehoeren.
- Diese Jahres-Zaehlungen werden in einer Count-CSV gespeichert.

## Ergebnisdateien
- `*_urls_*.csv` enthaelt alle gefundenen Artikel-Links.
- `*_counts_*.csv` enthaelt die Auswertung pro Jahr.
