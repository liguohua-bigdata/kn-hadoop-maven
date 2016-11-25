package pa2.util;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;

public class XmlUtil {
    /**
     * get content by tagname
     *
     * @param xml xml
     * @return content
     */
    public static String getConentByTagNameText(String xml) {
        return getConentByTagName(xml, "text");
    }

    /**
     * get content by tagname
     *
     * @param xml     xml
     * @param tagName tagName
     * @return content
     */
    public static String getConentByTagName(String xml, String tagName) {
        return getConentByTagName(xml, tagName, "UTF-8");
    }

    /**
     * get content by tagname
     *
     * @param xml        xml
     * @param tagName    tagName
     * @param enCodeType enCodeType
     * @return content
     */
    public static String getConentByTagName(String xml, String tagName, String enCodeType) {
        String contentInTag = "";
        XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
        XMLStreamReader xmlStreamReader = null;
        try {
            xmlStreamReader = xmlInputFactory.createXMLStreamReader(new ByteArrayInputStream(xml.getBytes(enCodeType)));
            while (xmlStreamReader.hasNext()) {
                int event = xmlStreamReader.next();
                //1.check the docment
                if (event != XMLStreamConstants.START_ELEMENT) {
                    continue;
                }
                if (!xmlStreamReader.getLocalName().equals(tagName)) {
                    continue;
                }
                //2.get target tag
                if (!xmlStreamReader.isEndElement()) {
                    break;
                }
            }
            contentInTag = xmlStreamReader.getElementText();
        } catch (XMLStreamException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return contentInTag;
    }
}
