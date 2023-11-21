package datawarehouse;

import java.io.IOException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class CrawlData {

	public static void main(String[] args) throws IOException {
		String link = "https://www.pnj.com.vn/blog/gia-vang/";
		Document doc = Jsoup.connect(link).timeout(5000).get();
		Elements links = doc.select("tr");
		System.out.println("Loai Vang----------GiaBan-GiaMua");
		for(Element li:links) {
			String title = li.select("td").text();			
			System.out.println(title);		
		}
	}
}