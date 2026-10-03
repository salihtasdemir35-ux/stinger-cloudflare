# STINGER – Online sunucu (Cloudflare Workers, ücretsiz)

Liderlik panoları, hızlı maç, turnuvalar, canlı izleme ve sohbet.
Kart gerektirmez. Liderlik kayıtları Cloudflare'in kendi veritabanında kalıcı tutulur.

Kurulum: Cloudflare → Workers & Pages → Create → Import a repository → bu depo → Deploy.
Worker adı `stinger-sunucu` olmalı (wrangler.jsonc ile aynı).
Adres: `https://stinger-sunucu.<alt-alan-adın>.workers.dev` → oyunda `wss://...` olarak kullanılır.
