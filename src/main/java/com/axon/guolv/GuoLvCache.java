package com.axon.guolv;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.hadoop.compression.lzo.LzopCodec;

public class GuoLvCache {
	private static Logger logger = Logger.getLogger(GuoLv.class);

	public static void main(String[] args) {
		Configuration configuration = new Configuration();
		Job job = null;
		try {
			job = Job.getInstance(configuration);
			job.setJarByClass(GuoLv.class);

			job.addCacheFile(new URI(
					"hdfs://10.10.136.50:9000/history/jiangsu/conf/part-m-00000"));

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(GuoLvBean.class);
			job.setMapperClass(GuoLvCacheMapper.class);

			job.setReducerClass(GuoLvCacheRedeucer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			// job.setCombinerClass(GuoLvRedeucer.class);
			job.setNumReduceTasks(1);

			FileInputFormat.addInputPath(job, new Path(
					"/history/jiangsu/part-m-00000"));
			FileOutputFormat.setOutputPath(job, new Path(
					"/history/guolv/jiangsu"));
			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
			job.waitForCompletion(true);

		} catch (IOException e) {
			logger.error("job初始化失败", e);
		} catch (ClassNotFoundException e) {
			logger.error("类没找到异常", e);
		} catch (InterruptedException e) {
			logger.error("中断异常", e);
		} catch (URISyntaxException e) {
			logger.error("uri不存在", e);
		}

	}

	public static class GuoLvCacheMapper extends
			Mapper<LongWritable, Text, Text, GuoLvBean> {
		private HashMap<String, String> confHm = new HashMap<String, String>();

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, GuoLvBean>.Context context)
				throws IOException, InterruptedException {
			// 1.获取每行的数据放到字符串数组中
			// phone,terminal_id,start_time,end_time,AREANO,imei
			// 0 1 2 3 4 5
			String[] str = value.toString().split(",");
			// 2.从confHm中获取相应phone的注册时间进行比较（使用毫秒数进行减法> 0 的过滤数据）
			if (str.length == 6 && StringUtils.isNotBlank(str[0])
					&& StringUtils.isNotBlank(str[1])
					&& StringUtils.isNotBlank(str[2])
					&& StringUtils.isNotBlank(str[3])
					&& StringUtils.isNotBlank(str[4])
					&& StringUtils.isNotBlank(str[5])) {
				// logger.info("terminal_id"+str[1]);
				String openDateString = confHm.get(str[0].trim());
				if (!StringUtils.isNotBlank(openDateString))
					return;
				long openDate = getTime(openDateString);
				long startTime = getTime(str[2].trim());
				if (startTime - openDate > 0)
					return;
				GuoLvBean glb = new GuoLvBean();
				glb.setPhone(str[0]);
				glb.setTerminalId(str[1]);
				glb.setStartTime(str[2]);
				glb.setEndTime(str[3]);
				glb.setAreaNo(str[4]);
				glb.setImei(str[5]);

				context.write(new Text(str[0]), glb);
			}
		}

		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, GuoLvBean>.Context context)
				throws IOException, InterruptedException {
			logger.info("开始setup");

			// java.net的uri
			URI[] cacheFiles = context.getCacheFiles();
			Path path = new Path(cacheFiles[0]);
			FileSystem fs = FileSystem.get(cacheFiles[0], new Configuration());
			InputStream in = fs.open(path);
			List<String> list = IOUtils.readLines(in);
			for (String line : list) {
				String[] str = line.split("\\s+");
				confHm.put(str[0].trim(), str[1].trim());
			}
		}

		private long getTime(String openDate) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date date = null;
			try {
				date = sdf.parse(openDate);
			} catch (ParseException e) {
				logger.error("日期格式不对", e);
			}
			return date.getTime();
		}
	}

	public static class GuoLvCacheRedeucer extends
			Reducer<Text, GuoLvBean, Text, Text> {
		@Override
		protected void reduce(Text text, Iterable<GuoLvBean> values,
				Reducer<Text, GuoLvBean, Text, Text>.Context context)
				throws IOException, InterruptedException {
			logger.info("开始reduce 进程");
			// phone,terminal_id,start_time,end_time,AREANO,imei
			// 0 1 2 3 4 5
			for (GuoLvBean glb : values) {

				context.write(
						text,
						new Text(glb.getTerminalId() + "\t"
								+ glb.getStartTime() + "\t" + glb.getEndTime()
								+ "\t" + glb.getAreaNo() + "\t" + glb.getImei()));
			}
		}

	}

}
