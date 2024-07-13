package cn.com.paic.text2audio;

import com.jacob.activeX.ActiveXComponent;
import com.jacob.com.Dispatch;
import com.jacob.com.Variant;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class MediaDemoApplication {
    public static void main(String[] args) {
        try {
            List<String> lines = Files.readAllLines(Paths.get("F:\\datas\\input\\测试文件.txt"), Charset.forName("GBK"));
            for (String line : lines) {
                System.out.println(line);
                textToSpeech(line,"F:\\datas\\");
            }

            //textToSpeech("你好，我叫小马，今年十岁了。my first name is ma", "F:\\datas\\");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @author
     * @date: 2019年
     * 文字转语音并生成语音文件方法
     * input：	data：需要转的文字对象，path：语音文件保存位置对象
     */
    public static void textToSpeech(String data, String path) {
        ActiveXComponent ax = null;
        try {
            ax = new ActiveXComponent("Sapi.SpVoice");

            // 运行时输出语音内容
            Dispatch spVoice = ax.getObject();
            // 设置音量大小 0-100
            ax.setProperty("Volume", new Variant(100));
            // 设置语音朗读速度 -10 到 +10
            ax.setProperty("Rate", new Variant(-2));
            // 执行朗读，发声
            Dispatch.call(spVoice, "Speak", new Variant(data));

            // 下面是构建文件流把生成语音文件
            ax = new ActiveXComponent("Sapi.SpFileStream");
            Dispatch spFileStream = ax.getObject();

            ax = new ActiveXComponent("Sapi.SpAudioFormat");
            Dispatch spAudioFormat = ax.getObject();

            // 设置音频流格式
            Dispatch.put(spAudioFormat, "Type", new Variant(22));
            // 设置文件输出流格式
            Dispatch.putRef(spFileStream, "Format", spAudioFormat);
            // 调用输出 文件流打开方法，创建一个.wav文件
            Dispatch.call(spFileStream, "Open", new Variant(path + "/voice.wav"), new Variant(3), new Variant(true));
            // 设置声音对象的音频输出流为输出文件对象
            Dispatch.putRef(spVoice, "AudioOutputStream", spFileStream);
            // 设置音量 0到100
            Dispatch.put(spVoice, "Volume", new Variant(100));
            // 设置朗读速度
            Dispatch.put(spVoice, "Rate", new Variant(-2));
            // 开始朗读
            Dispatch.call(spVoice, "Speak", new Variant(data));

            // 关闭输出文件
            Dispatch.call(spFileStream, "Close");
            Dispatch.putRef(spVoice, "AudioOutputStream", null);

            spAudioFormat.safeRelease();
            spFileStream.safeRelease();
            spVoice.safeRelease();
            ax.safeRelease();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
