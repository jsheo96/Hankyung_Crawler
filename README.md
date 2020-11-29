# Hankyung_Crawler

한국경제 뉴스 크롤러

# 사용법

git clone ...

hankyung = HankyungCrawler() 한국 경제 크롤러 초기화. db 파일 생성

hankyung.update(1) 최근 1일간 한국경제 뉴스 크롤링

news_list = hankyung.get() 크롤링된 뉴스들을 list 형태로 저장

각각의 뉴스는 튜플로 저장됨
(날짜, 분야, 제목, 내용, url)



hankyung.update_threaded(n,m) 최근 n일간 한국경제 뉴스 크롤링 (스레드 m개 사용)

hankyung.delete_records() db 파일 내용 삭제


