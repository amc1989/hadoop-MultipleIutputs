package com.axon.guolv;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.hadoop.compression.lzo.LzopCodec;

public class GuoLv {
	private static Logger logger = Logger.getLogger(GuoLv.class);

	public static void main(String[] args) {
		Configuration configuration = new Configuration();
		Job job = null;
		try {
			job = Job.getInstance(configuration);
			job.setJarByClass(GuoLv.class);

			/*
			 * job.addCacheFile(new URI(
			 * "hdfs://10.10.136.50:9000/history/jiangsu/conf/part-m-00000"));
			 */
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(GuoLvBean.class);
			job.setMapperClass(GuoLvMapper.class);

			job.setReducerClass(GuoLvRedeucer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			// job.setCombinerClass(GuoLvRedeucer.class);
			job.setNumReduceTasks(4);
			MultipleInputs.addInputPath(job, new Path(
					"/history/jiangsu/conf/part-m-00000"),
					TextInputFormat.class, GuoLvConfMapper.class);
			MultipleInputs.addInputPath(job, new Path(
					"/history/jiangsu/part-m-00000"), TextInputFormat.class,
					GuoLvMapper.class);

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
		}

	}

	public static class GuoLvConfMapper extends
			Mapper<LongWritable, Text, Text, GuoLvBean> {

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, GuoLvBean>.Context context)
				throws IOException, InterruptedException {
			String[] str = value.toString().split(",");
			GuoLvBean glb = new GuoLvBean();
			glb.setPhone(str[0].trim());
			glb.setStartTime("conf," + str[1]);
			glb.setEndTime("2");
			glb.setTerminalId("3");
			glb.setAreaNo("111");
			glb.setImei("fff");
			context.write(new Text(str[0].trim()), glb);
		}

	}

	public static class GuoLvMapper extends
			Mapper<LongWritable, Text, Text, GuoLvBean> {
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
	}

	public static class GuoLvRedeucer extends
			Reducer<Text, GuoLvBean, Text, Text> {
		private Configuration conf = new Configuration();

		@Override
		protected void reduce(Text text, Iterable<GuoLvBean> values,
				Reducer<Text, GuoLvBean, Text, Text>.Context context)
				throws IOException, InterruptedException {
			logger.info("开始reduce 进程");
			 List<GuoLvBean> list = new ArrayList<GuoLvBean>();
			 String openDate=null;;
			// phone,terminal_id,start_time,end_time,AREANO,imei
			// 0 1 2 3 4 5

			for (GuoLvBean glb : values) {
				if (glb.getStartTime().startsWith("conf,")) {
					logger.info("配置表的日期  ：   " + glb.getStartTime());
					String[] openDte = glb.getStartTime().split(",");
					if (openDte.length == 2
							&& StringUtils.isNotBlank(openDte[1])) {
						openDate = openDte[1];
					} else {
						return;
					}
				} else {
					logger.info("历史表的日期  ：   " + glb.toString());
					list.add(WritableUtils.clone(glb, conf));
				}

			}
			for (GuoLvBean glb : list) {
				long starttime = getTime(glb.getStartTime());
				long opentime = 0;
				if (StringUtils.isNotBlank(openDate)) {
					opentime = getTime(openDate);
				} else {
					return;
				}
				if (starttime - opentime > 0) {
					return;
				} else {
					context.write(
							text,
							new Text(glb.getPhone() + "\t"
									+ glb.getTerminalId() + "\t"
									+ glb.getStartTime() + "\t"
									+ glb.getEndTime() + "\t" + glb.getAreaNo()
									+ "\t" + glb.getImei()));
				}
			}
			list.clear();
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

}
