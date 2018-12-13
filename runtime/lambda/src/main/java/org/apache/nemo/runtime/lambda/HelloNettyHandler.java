package org.apache.nemo.runtime.lambda;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.nemo.common.EventHandler;
import org.apache.nemo.common.NemoEvent;
import org.apache.nemo.common.NettyChannelInitializer;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.punctuation.Watermark;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class HelloNettyHandler implements RequestHandler<Map<String, Object>, Object> {

	private static final Logger LOG = LogManager.getLogger(HelloNettyHandler.class);
	//private static final OutputSender sender = new OutputSender("18.182.129.182", 20312);
	private static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
    .withClientConfiguration(new ClientConfiguration().withMaxConnections(100)).build();

	private static final String BUCKET_NAME = "nemo-serverless";
	private static final String PATH = "/tmp/nexmark-0.2-SNAPSHOT-shaded.jar";
	//private static final String PATH = "/tmp/shaded.jar";
	private URLClassLoader classLoader = null;
	private OperatorVertex irVertex = null;

	//private final String serializedUserCode = "rO0ABXNyABZRdWVyeTdTaWRlSW5wdXRIYW5kbGVyMlM6Ib0vAkQCAAB4cA==";
  private final String serializedCodeQuery7 = "rO0ABXNyAC9vcmcuYXBhY2hlLm5lbW8uY29tbW9uLmlyLnZlcnRleC5PcGVyYXRvclZlcnRleANuczBdOJeAAgABTAAJdHJhbnNmb3JtdAA2TG9yZy9hcGFjaGUvbmVtby9jb21tb24vaXIvdmVydGV4L3RyYW5zZm9ybS9UcmFuc2Zvcm07eHIAKW9yZy5hcGFjaGUubmVtby5jb21tb24uaXIudmVydGV4LklSVmVydGV4GD8CYD0sxkACAAJaABBzdGFnZVBhcnRpdGlvbmVkTAATZXhlY3V0aW9uUHJvcGVydGllc3QAQkxvcmcvYXBhY2hlL25lbW8vY29tbW9uL2lyL2V4ZWN1dGlvbnByb3BlcnR5L0V4ZWN1dGlvblByb3BlcnR5TWFwO3hyACFvcmcuYXBhY2hlLm5lbW8uY29tbW9uLmRhZy5WZXJ0ZXirmbwYrvuHqgIAAUwAAmlkdAASTGphdmEvbGFuZy9TdHJpbmc7eHB0AAh2ZXJ0ZXgxNABzcgBAb3JnLmFwYWNoZS5uZW1vLmNvbW1vbi5pci5leGVjdXRpb25wcm9wZXJ0eS5FeGVjdXRpb25Qcm9wZXJ0eU1hcNLNJu+xp+KlAgADTAATZmluYWxpemVkUHJvcGVydGllc3QAD0xqYXZhL3V0aWwvU2V0O0wAAmlkcQB+AAVMAApwcm9wZXJ0aWVzdAAPTGphdmEvdXRpbC9NYXA7eHBzcgARamF2YS51dGlsLkhhc2hTZXS6RIWVlri3NAMAAHhwdwwAAAABP0AAAAAAAAB4cQB+AAdzcgARamF2YS51dGlsLkhhc2hNYXAFB9rBwxZg0QMAAkYACmxvYWRGYWN0b3JJAAl0aHJlc2hvbGR4cD9AAAAAAAAMdwgAAAAQAAAABnZyAEdvcmcuYXBhY2hlLm5lbW8uY29tbW9uLmlyLnZlcnRleC5leGVjdXRpb25wcm9wZXJ0eS5SZXNvdXJjZVNsb3RQcm9wZXJ0eTUuJDL0mCcEAgAAeHIAQ29yZy5hcGFjaGUubmVtby5jb21tb24uaXIuZXhlY3V0aW9ucHJvcGVydHkuVmVydGV4RXhlY3V0aW9uUHJvcGVydHmp/jnY0n2c7QIAAHhyAD1vcmcuYXBhY2hlLm5lbW8uY29tbW9uLmlyLmV4ZWN1dGlvbnByb3BlcnR5LkV4ZWN1dGlvblByb3BlcnR5D1RCiV69ZOsCAAFMAAV2YWx1ZXQAFkxqYXZhL2lvL1NlcmlhbGl6YWJsZTt4cHNxAH4AEHNyABFqYXZhLmxhbmcuQm9vbGVhbs0gcoDVnPruAgABWgAFdmFsdWV4cAF2cgBGb3JnLmFwYWNoZS5uZW1vLmNvbW1vbi5pci52ZXJ0ZXguZXhlY3V0aW9ucHJvcGVydHkuUGFyYWxsZWxpc21Qcm9wZXJ0ebGQ73eYKEvvAgAAeHEAfgARc3EAfgAYc3IAEWphdmEubGFuZy5JbnRlZ2VyEuKgpPeBhzgCAAFJAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAAAAAF2cgBHb3JnLmFwYWNoZS5uZW1vLmNvbW1vbi5pci52ZXJ0ZXguZXhlY3V0aW9ucHJvcGVydHkuUmVzb3VyY2VTaXRlUHJvcGVydHmsXkd2V/+5NQIAAHhxAH4AEXNxAH4AHnNxAH4ADj9AAAAAAAAAdwgAAAAQAAAAAHh2cgBLb3JnLmFwYWNoZS5uZW1vLmNvbW1vbi5pci52ZXJ0ZXguZXhlY3V0aW9ucHJvcGVydHkuUmVzb3VyY2VMb2NhbGl0eVByb3BlcnR5YMr5t3uAXV0CAAB4cQB+ABFzcQB+ACJxAH4AF3ZyAEtvcmcuYXBhY2hlLm5lbW8uY29tbW9uLmlyLnZlcnRleC5leGVjdXRpb25wcm9wZXJ0eS5SZXNvdXJjZVByaW9yaXR5UHJvcGVydHmXtFFXgKC9uwIAAHhxAH4AEXNxAH4AJXQABE5vbmV2cgBIb3JnLmFwYWNoZS5uZW1vLmNvbW1vbi5pci52ZXJ0ZXguZXhlY3V0aW9ucHJvcGVydHkuU2NoZWR1bGVHcm91cFByb3BlcnR5YkHi1coiikgCAAB4cQB+ABFzcQB+AClzcQB+ABsAAAADeHNyAEZvcmcuYXBhY2hlLm5lbW8uY29tcGlsZXIuZnJvbnRlbmQuYmVhbS50cmFuc2Zvcm0uUHVzaEJhY2tEb0ZuVHJhbnNmb3JtVwXIOqxrRYQCAARKABFjdXJJbnB1dFdhdGVybWFya0oAEmN1ck91dHB1dFdhdGVybWFya0oAFmN1clB1c2hlZEJhY2tXYXRlcm1hcmtMAA5jdXJQdXNoZWRCYWNrc3QAEExqYXZhL3V0aWwvTGlzdDt4cgBGb3JnLmFwYWNoZS5uZW1vLmNvbXBpbGVyLmZyb250ZW5kLmJlYW0udHJhbnNmb3JtLkFic3RyYWN0RG9GblRyYW5zZm9ybdibAfu+ajp7AgAMWgAOYnVuZGxlRmluaXNoZWRKAA9jdXJyQnVuZGxlQ291bnRKABNwcmV2QnVuZGxlU3RhcnRUaW1lTAAUYWRkaXRpb25hbE91dHB1dFRhZ3NxAH4ALkwAC2Rpc3BsYXlEYXRhdAA0TG9yZy9hcGFjaGUvYmVhbS9zZGsvdHJhbnNmb3Jtcy9kaXNwbGF5L0Rpc3BsYXlEYXRhO0wABGRvRm50ACVMb3JnL2FwYWNoZS9iZWFtL3Nkay90cmFuc2Zvcm1zL0RvRm47TAAKaW5wdXRDb2RlcnQAIkxvcmcvYXBhY2hlL2JlYW0vc2RrL2NvZGVycy9Db2RlcjtMAA1tYWluT3V0cHV0VGFndAAlTG9yZy9hcGFjaGUvYmVhbS9zZGsvdmFsdWVzL1R1cGxlVGFnO0wADG91dHB1dENvZGVyc3EAfgAKTAARc2VyaWFsaXplZE9wdGlvbnN0AEdMb3JnL2FwYWNoZS9iZWFtL3J1bm5lcnMvY29yZS9jb25zdHJ1Y3Rpb24vU2VyaWFsaXphYmxlUGlwZWxpbmVPcHRpb25zO0wACnNpZGVJbnB1dHNxAH4ACkwAEXdpbmRvd2luZ1N0cmF0ZWd5dAAuTG9yZy9hcGFjaGUvYmVhbS9zZGsvdmFsdWVzL1dpbmRvd2luZ1N0cmF0ZWd5O3hwAQAAAAAAAAAAAAAAAAAAAABzcgAmamF2YS51dGlsLkNvbGxlY3Rpb25zJFVubW9kaWZpYWJsZUxpc3T8DyUxteyOEAIAAUwABGxpc3RxAH4ALnhyACxqYXZhLnV0aWwuQ29sbGVjdGlvbnMkVW5tb2RpZmlhYmxlQ29sbGVjdGlvbhlCAIDLXvceAgABTAABY3QAFkxqYXZhL3V0aWwvQ29sbGVjdGlvbjt4cHNyABNqYXZhLnV0aWwuQXJyYXlMaXN0eIHSHZnHYZ0DAAFJAARzaXpleHAAAAAAdwQAAAAAeHEAfgA8c3IAMm9yZy5hcGFjaGUuYmVhbS5zZGsudHJhbnNmb3Jtcy5kaXNwbGF5LkRpc3BsYXlEYXRh5flDfDqiXCoCAAFMAAdlbnRyaWVzdABXTG9yZy9hcGFjaGUvYmVhbS9yZXBhY2thZ2VkL2JlYW1fc2Rrc19qYXZhX2NvcmUvY29tL2dvb2dsZS9jb21tb24vY29sbGVjdC9JbW11dGFibGVNYXA7eHBzcgBmb3JnLmFwYWNoZS5iZWFtLnJlcGFja2FnZWQuYmVhbV9zZGtzX2phdmFfY29yZS5jb20uZ29vZ2xlLmNvbW1vbi5jb2xsZWN0LkltbXV0YWJsZUJpTWFwJFNlcmlhbGl6ZWRGb3JtAAAAAAAAAAACAAB4cgBkb3JnLmFwYWNoZS5iZWFtLnJlcGFja2FnZWQuYmVhbV9zZGtzX2phdmFfY29yZS5jb20uZ29vZ2xlLmNvbW1vbi5jb2xsZWN0LkltbXV0YWJsZU1hcCRTZXJpYWxpemVkRm9ybQAAAAAAAAAAAgACWwAEa2V5c3QAE1tMamF2YS9sYW5nL09iamVjdDtbAAZ2YWx1ZXNxAH4AQnhwdXIAE1tMamF2YS5sYW5nLk9iamVjdDuQzlifEHMpbAIAAHhwAAAAAXNyAEdvcmcuYXBhY2hlLmJlYW0uc2RrLnRyYW5zZm9ybXMuZGlzcGxheS5BdXRvVmFsdWVfRGlzcGxheURhdGFfSWRlbnRpZmllcus9Qs1NaI19AgADTAADa2V5cQB+AAVMAAluYW1lc3BhY2V0ABFMamF2YS9sYW5nL0NsYXNzO0wABHBhdGh0ADlMb3JnL2FwYWNoZS9iZWFtL3Nkay90cmFuc2Zvcm1zL2Rpc3BsYXkvRGlzcGxheURhdGEkUGF0aDt4cgA9b3JnLmFwYWNoZS5iZWFtLnNkay50cmFuc2Zvcm1zLmRpc3BsYXkuRGlzcGxheURhdGEkSWRlbnRpZmllcsEXqR+DvTaiAgAAeHB0AAJmbnZyADBvcmcuYXBhY2hlLmJlYW0uc2RrLnRyYW5zZm9ybXMuUGFyRG8kTXVsdGlPdXRwdXS5cQcEe1uJogIABUwAFGFkZGl0aW9uYWxPdXRwdXRUYWdzdAApTG9yZy9hcGFjaGUvYmVhbS9zZGsvdmFsdWVzL1R1cGxlVGFnTGlzdDtMAAJmbnEAfgAxTAANZm5EaXNwbGF5RGF0YXQAPUxvcmcvYXBhY2hlL2JlYW0vc2RrL3RyYW5zZm9ybXMvZGlzcGxheS9EaXNwbGF5RGF0YSRJdGVtU3BlYztMAA1tYWluT3V0cHV0VGFncQB+ADNMAApzaWRlSW5wdXRzcQB+AC54cgApb3JnLmFwYWNoZS5iZWFtLnNkay50cmFuc2Zvcm1zLlBUcmFuc2Zvcm0rFlVxiIlmxwMAAHhwc3IAN29yZy5hcGFjaGUuYmVhbS5zZGsudHJhbnNmb3Jtcy5kaXNwbGF5LkRpc3BsYXlEYXRhJFBhdGgKtqeZjvoyKQIAAUwACmNvbXBvbmVudHN0AFhMb3JnL2FwYWNoZS9iZWFtL3JlcGFja2FnZWQvYmVhbV9zZGtzX2phdmFfY29yZS9jb20vZ29vZ2xlL2NvbW1vbi9jb2xsZWN0L0ltbXV0YWJsZUxpc3Q7eHBzcgBlb3JnLmFwYWNoZS5iZWFtLnJlcGFja2FnZWQuYmVhbV9zZGtzX2phdmFfY29yZS5jb20uZ29vZ2xlLmNvbW1vbi5jb2xsZWN0LkltbXV0YWJsZUxpc3QkU2VyaWFsaXplZEZvcm0AAAAAAAAAAAIAAVsACGVsZW1lbnRzcQB+AEJ4cHVxAH4ARAAAAAB1cQB+AEQAAAABc3IAQW9yZy5hcGFjaGUuYmVhbS5zZGsudHJhbnNmb3Jtcy5kaXNwbGF5LkF1dG9WYWx1ZV9EaXNwbGF5RGF0YV9JdGVtab6WYLXC2acCAAhMAANrZXlxAH4ABUwABWxhYmVscQB+AAVMAAdsaW5rVXJscQB+AAVMAAluYW1lc3BhY2VxAH4AR0wABHBhdGhxAH4ASEwACnNob3J0VmFsdWV0ABJMamF2YS9sYW5nL09iamVjdDtMAAR0eXBldAA5TG9yZy9hcGFjaGUvYmVhbS9zZGsvdHJhbnNmb3Jtcy9kaXNwbGF5L0Rpc3BsYXlEYXRhJFR5cGU7TAAFdmFsdWVxAH4AWXhyADdvcmcuYXBhY2hlLmJlYW0uc2RrLnRyYW5zZm9ybXMuZGlzcGxheS5EaXNwbGF5RGF0YSRJdGVtAUjD5VOmibsCAAB4cHEAfgBLdAASVHJhbnNmb3JtIEZ1bmN0aW9ucHEAfgBQcQB+AFN0AAB+cgA3b3JnLmFwYWNoZS5iZWFtLnNkay50cmFuc2Zvcm1zLmRpc3BsYXkuRGlzcGxheURhdGEkVHlwZQAAAAAAAAAAEgAAeHIADmphdmEubGFuZy5FbnVtAAAAAAAAAAASAAB4cHQACkpBVkFfQ0xBU1N0ACxvcmcuYXBhY2hlLmJlYW0uc2RrLm5leG1hcmsucXVlcmllcy5RdWVyeTckMXNyACxvcmcuYXBhY2hlLmJlYW0uc2RrLm5leG1hcmsucXVlcmllcy5RdWVyeTckMdgEH3kvIULHAgACTAAGdGhpcyQwdAAsTG9yZy9hcGFjaGUvYmVhbS9zZGsvbmV4bWFyay9xdWVyaWVzL1F1ZXJ5NztMABB2YWwkbWF4UHJpY2VWaWV3dAAsTG9yZy9hcGFjaGUvYmVhbS9zZGsvdmFsdWVzL1BDb2xsZWN0aW9uVmlldzt4cgAjb3JnLmFwYWNoZS5iZWFtLnNkay50cmFuc2Zvcm1zLkRvRm6sRCMGzpO9nQIAAHhwc3IAKm9yZy5hcGFjaGUuYmVhbS5zZGsubmV4bWFyay5xdWVyaWVzLlF1ZXJ5N0lEpeQHr1ReAgAAeHIAMG9yZy5hcGFjaGUuYmVhbS5zZGsubmV4bWFyay5xdWVyaWVzLk5leG1hcmtRdWVyeRcPFU5jPooTAgAFTAANY29uZmlndXJhdGlvbnQAMkxvcmcvYXBhY2hlL2JlYW0vc2RrL25leG1hcmsvTmV4bWFya0NvbmZpZ3VyYXRpb247TAASZW5kT2ZTdHJlYW1Nb25pdG9ydAAlTG9yZy9hcGFjaGUvYmVhbS9zZGsvbmV4bWFyay9Nb25pdG9yO0wADGV2ZW50TW9uaXRvcnEAfgBsTAAMZmF0YWxDb3VudGVydAAlTG9yZy9hcGFjaGUvYmVhbS9zZGsvbWV0cmljcy9Db3VudGVyO0wADXJlc3VsdE1vbml0b3JxAH4AbHhxAH4AT3hzcgAwb3JnLmFwYWNoZS5iZWFtLnNkay5uZXhtYXJrLk5leG1hcmtDb25maWd1cmF0aW9uSMffQeYSyHICACdJAAthdWN0aW9uU2tpcEkAEmF2Z0F1Y3Rpb25CeXRlU2l6ZUkADmF2Z0JpZEJ5dGVTaXplSQARYXZnUGVyc29uQnl0ZVNpemVKAApjcHVEZWxheU1zWgAFZGVidWdKAA1kaXNrQnVzeUJ5dGVzWgAXZXhwb3J0U3VtbWFyeVRvQmlnUXVlcnlJAAZmYW5vdXRJAA5maXJzdEV2ZW50UmF0ZUkAD2hvdEF1Y3Rpb25SYXRpb0kAD2hvdEJpZGRlcnNSYXRpb0kAD2hvdFNlbGxlcnNSYXRpb1oADWlzUmF0ZUxpbWl0ZWRJABZtYXhBdWN0aW9uc1dhaXRpbmdUaW1lSQAMbWF4TG9nRXZlbnRzSQANbmV4dEV2ZW50UmF0ZUkAD251bUFjdGl2ZVBlb3BsZUkAEm51bUV2ZW50R2VuZXJhdG9yc0oACW51bUV2ZW50c0kAE251bUluRmxpZ2h0QXVjdGlvbnNKABJvY2Nhc2lvbmFsRGVsYXlTZWNKABNvdXRPZk9yZGVyR3JvdXBTaXplSQAOcHJlbG9hZFNlY29uZHNEABBwcm9iRGVsYXllZEV2ZW50SQAFcXVlcnlJAA1yYXRlUGVyaW9kU2VjSQANc3RyZWFtVGltZW91dFoAFHVzZVB1YnN1YlB1Ymxpc2hUaW1lWgAVdXNlV2FsbGNsb2NrRXZlbnRUaW1lSgAUd2F0ZXJtYXJrSG9sZGJhY2tTZWNKAA93aW5kb3dQZXJpb2RTZWNKAA13aW5kb3dTaXplU2VjTAANY29kZXJTdHJhdGVneXQAOExvcmcvYXBhY2hlL2JlYW0vc2RrL25leG1hcmsvTmV4bWFya1V0aWxzJENvZGVyU3RyYXRlZ3k7TAAKcHViU3ViTW9kZXQANUxvcmcvYXBhY2hlL2JlYW0vc2RrL25leG1hcmsvTmV4bWFya1V0aWxzJFB1YlN1Yk1vZGU7TAAJcmF0ZVNoYXBldAA0TG9yZy9hcGFjaGUvYmVhbS9zZGsvbmV4bWFyay9OZXhtYXJrVXRpbHMkUmF0ZVNoYXBlO0wACHJhdGVVbml0dAAzTG9yZy9hcGFjaGUvYmVhbS9zZGsvbmV4bWFyay9OZXhtYXJrVXRpbHMkUmF0ZVVuaXQ7TAAIc2lua1R5cGV0ADNMb3JnL2FwYWNoZS9iZWFtL3Nkay9uZXhtYXJrL05leG1hcmtVdGlscyRTaW5rVHlwZTtMAApzb3VyY2VUeXBldAA1TG9yZy9hcGFjaGUvYmVhbS9zZGsvbmV4bWFyay9OZXhtYXJrVXRpbHMkU291cmNlVHlwZTt4cAAAAHsAAAH0AAAAZAAAAMgAAAAAAAAAAAEAAAAAAAAAAAAAAAABAAAAZAAAAAIAAAAEAAAABAEAAAJYAAGGoAAAAGQAAAPoAAAAAQAAAAACYloAAAAAZAAAAAAAAAADAAAAAAAAAAEAAAAAP7mZmZmZmZoAAAAHAAACWAAAAZAAAAAAAAAAAAAAAAAAAAAAAAUAAAAAAAAAHn5yADZvcmcuYXBhY2hlLmJlYW0uc2RrLm5leG1hcmsuTmV4bWFya1V0aWxzJENvZGVyU3RyYXRlZ3kAAAAAAAAAABIAAHhxAH4AYHQABEhBTkR+cgAzb3JnLmFwYWNoZS5iZWFtLnNkay5uZXhtYXJrLk5leG1hcmtVdGlscyRQdWJTdWJNb2RlAAAAAAAAAAASAAB4cQB+AGB0AAhDT01CSU5FRH5yADJvcmcuYXBhY2hlLmJlYW0uc2RrLm5leG1hcmsuTmV4bWFya1V0aWxzJFJhdGVTaGFwZQAAAAAAAAAAEgAAeHEAfgBgdAAEU0lORX5yADFvcmcuYXBhY2hlLmJlYW0uc2RrLm5leG1hcmsuTmV4bWFya1V0aWxzJFJhdGVVbml0AAAAAAAAAAASAAB4cQB+AGB0AApQRVJfU0VDT05EfnIAMW9yZy5hcGFjaGUuYmVhbS5zZGsubmV4bWFyay5OZXhtYXJrVXRpbHMkU2lua1R5cGUAAAAAAAAAABIAAHhxAH4AYHQAB0RFVk5VTEx+cgAzb3JnLmFwYWNoZS5iZWFtLnNkay5uZXhtYXJrLk5leG1hcmtVdGlscyRTb3VyY2VUeXBlAAAAAAAAAAASAAB4cQB+AGB0AAZESVJFQ1RzcgAjb3JnLmFwYWNoZS5iZWFtLnNkay5uZXhtYXJrLk1vbml0b3J6rqYGC2MuAAIABEwABGRvRm50ADFMb3JnL2FwYWNoZS9iZWFtL3Nkay9uZXhtYXJrL01vbml0b3IkTW9uaXRvckRvRm47TAAEbmFtZXEAfgAFTAAGcHJlZml4cQB+AAVMAAl0cmFuc2Zvcm10ACtMb3JnL2FwYWNoZS9iZWFtL3Nkay90cmFuc2Zvcm1zL1BUcmFuc2Zvcm07eHBzcgAvb3JnLmFwYWNoZS5iZWFtLnNkay5uZXhtYXJrLk1vbml0b3IkTW9uaXRvckRvRm5rY6jErP7r8QIAB0wADGJ5dGVzQ291bnRlcnEAfgBtTAAOZWxlbWVudENvdW50ZXJxAH4AbUwAB2VuZFRpbWV0ACpMb3JnL2FwYWNoZS9iZWFtL3Nkay9tZXRyaWNzL0Rpc3RyaWJ1dGlvbjtMAAxlbmRUaW1lc3RhbXBxAH4AjkwACXN0YXJ0VGltZXEAfgCOTAAOc3RhcnRUaW1lc3RhbXBxAH4AjkwABnRoaXMkMHEAfgBseHEAfgBnc3IANW9yZy5hcGFjaGUuYmVhbS5zZGsubWV0cmljcy5NZXRyaWNzJERlbGVnYXRpbmdDb3VudGVy6qDQfnuQHM8CAAFMAARuYW1ldAAoTG9yZy9hcGFjaGUvYmVhbS9zZGsvbWV0cmljcy9NZXRyaWNOYW1lO3hwc3IAMG9yZy5hcGFjaGUuYmVhbS5zZGsubWV0cmljcy5BdXRvVmFsdWVfTWV0cmljTmFtZcBF2uoeElxxAgACTAAEbmFtZXEAfgAFTAAJbmFtZXNwYWNlcQB+AAV4cgAmb3JnLmFwYWNoZS5iZWFtLnNkay5tZXRyaWNzLk1ldHJpY05hbWWKckhJ/y6ahQIAAHhwdAAJZW5kLmJ5dGVzdAASUXVlcnk3LkVuZE9mU3RyZWFtc3EAfgCQc3EAfgCTdAAMZW5kLmVsZW1lbnRzcQB+AJdzcgA6b3JnLmFwYWNoZS5iZWFtLnNkay5tZXRyaWNzLk1ldHJpY3MkRGVsZWdhdGluZ0Rpc3RyaWJ1dGlvbgm+MBk0i9c7AgABTAAEbmFtZXEAfgCReHBzcQB+AJN0AAtlbmQuZW5kVGltZXEAfgCXc3EAfgCbc3EAfgCTdAAQZW5kLmVuZFRpbWVzdGFtcHEAfgCXc3EAfgCbc3EAfgCTdAANZW5kLnN0YXJ0VGltZXEAfgCXc3EAfgCbc3EAfgCTdAASZW5kLnN0YXJ0VGltZXN0YW1wcQB+AJdxAH4AjHEAfgCXdAADZW5kc3IAMW9yZy5hcGFjaGUuYmVhbS5zZGsudHJhbnNmb3Jtcy5QYXJEbyRTaW5nbGVPdXRwdXQOX9TExWoyLgIAA0wAAmZucQB+ADFMAA1mbkRpc3BsYXlEYXRhcQB+AE5MAApzaWRlSW5wdXRzcQB+AC54cQB+AE94cQB+AI9zcgBFb3JnLmFwYWNoZS5iZWFtLnNkay50cmFuc2Zvcm1zLmRpc3BsYXkuQXV0b1ZhbHVlX0Rpc3BsYXlEYXRhX0l0ZW1TcGVjMLUSU8TR+lECAAdMAANrZXlxAH4ABUwABWxhYmVscQB+AAVMAAdsaW5rVXJscQB+AAVMAAluYW1lc3BhY2VxAH4AR0wACnNob3J0VmFsdWVxAH4AWUwABHR5cGVxAH4AWkwABXZhbHVlcQB+AFl4cgA7b3JnLmFwYWNoZS5iZWFtLnNkay50cmFuc2Zvcm1zLmRpc3BsYXkuRGlzcGxheURhdGEkSXRlbVNwZWOsRCDbMT8ZAgIAAHhwcQB+AEtxAH4AXXBwdAALTW9uaXRvckRvRm5xAH4AYXQAL29yZy5hcGFjaGUuYmVhbS5zZGsubmV4bWFyay5Nb25pdG9yJE1vbml0b3JEb0Zuc3IAH2phdmEudXRpbC5Db2xsZWN0aW9ucyRFbXB0eUxpc3R6uBe0PKee3gIAAHhwc3EAfgCJc3EAfgCNc3EAfgCQc3EAfgCTdAALZXZlbnQuYnl0ZXN0AA1RdWVyeTcuRXZlbnRzc3EAfgCQc3EAfgCTdAAOZXZlbnQuZWxlbWVudHNxAH4At3NxAH4Am3NxAH4Ak3QADWV2ZW50LmVuZFRpbWVxAH4At3NxAH4Am3NxAH4Ak3QAEmV2ZW50LmVuZFRpbWVzdGFtcHEAfgC3c3EAfgCbc3EAfgCTdAAPZXZlbnQuc3RhcnRUaW1lcQB+ALdzcQB+AJtzcQB+AJN0ABRldmVudC5zdGFydFRpbWVzdGFtcHEAfgC3cQB+ALJxAH4At3QABWV2ZW50c3EAfgCpeHEAfgCzc3EAfgCrcQB+AEtxAH4AXXBwdAALTW9uaXRvckRvRm5xAH4AYXEAfgCvcQB+ALFzcQB+AJBzcQB+AJN0AAVmYXRhbHQABlF1ZXJ5N3NxAH4AiXNxAH4AjXNxAH4AkHNxAH4Ak3QADHJlc3VsdC5ieXRlc3QADlF1ZXJ5Ny5SZXN1bHRzc3EAfgCQc3EAfgCTdAAPcmVzdWx0LmVsZW1lbnRzcQB+ANRzcQB+AJtzcQB+AJN0AA5yZXN1bHQuZW5kVGltZXEAfgDUc3EAfgCbc3EAfgCTdAATcmVzdWx0LmVuZFRpbWVzdGFtcHEAfgDUc3EAfgCbc3EAfgCTdAAQcmVzdWx0LnN0YXJ0VGltZXEAfgDUc3EAfgCbc3EAfgCTdAAVcmVzdWx0LnN0YXJ0VGltZXN0YW1wcQB+ANRxAH4Az3EAfgDUdAAGcmVzdWx0c3EAfgCpeHEAfgDQc3EAfgCrcQB+AEtxAH4AXXBwdAALTW9uaXRvckRvRm5xAH4AYXEAfgCvcQB+ALFzcgBBb3JnLmFwYWNoZS5iZWFtLnNkay52YWx1ZXMuUENvbGxlY3Rpb25WaWV3cyRTaW1wbGVQQ29sbGVjdGlvblZpZXfkPb0FxgdPnwIABUwABWNvZGVycQB+ADJMAAN0YWdxAH4AM0wABnZpZXdGbnQAJ0xvcmcvYXBhY2hlL2JlYW0vc2RrL3RyYW5zZm9ybXMvVmlld0ZuO0wAD3dpbmRvd01hcHBpbmdGbnQAOkxvcmcvYXBhY2hlL2JlYW0vc2RrL3RyYW5zZm9ybXMvd2luZG93aW5nL1dpbmRvd01hcHBpbmdGbjtMABF3aW5kb3dpbmdTdHJhdGVneXEAfgA1eHBzcgAib3JnLmFwYWNoZS5iZWFtLnNkay5jb2RlcnMuS3ZDb2RlcmoAvbkdo8o2AgACTAAIa2V5Q29kZXJxAH4AMkwACnZhbHVlQ29kZXJxAH4AMnhyACpvcmcuYXBhY2hlLmJlYW0uc2RrLmNvZGVycy5TdHJ1Y3R1cmVkQ29kZXJzvxIO1dQ2EQIAAHhyACBvcmcuYXBhY2hlLmJlYW0uc2RrLmNvZGVycy5Db2RlckPd1YmuvH74AgAAeHBzcgAkb3JnLmFwYWNoZS5iZWFtLnNkay5jb2RlcnMuVm9pZENvZGVyub9Vm+gNr1UCAAB4cgAmb3JnLmFwYWNoZS5iZWFtLnNkay5jb2RlcnMuQXRvbWljQ29kZXLH7LXMhXRQRgIAAHhxAH4A7XNyACdvcmcuYXBhY2hlLmJlYW0uc2RrLmNvZGVycy5WYXJMb25nQ29kZXJcbNbDTf5NaQIAAHhxAH4A7XNyACNvcmcuYXBhY2hlLmJlYW0uc2RrLnZhbHVlcy5UdXBsZVRhZ7MYeWZbwHq1AgACWgAJZ2VuZXJhdGVkTAACaWRxAH4ABXhwAXQAXW9yZy5hcGFjaGUuYmVhbS5zZGsudmFsdWVzLlBDb2xsZWN0aW9uVmlld3MkU2ltcGxlUENvbGxlY3Rpb25WaWV3Ljxpbml0PjozOTAjMjU4N2FmOTdiNDg2NTUzOHNyADtvcmcuYXBhY2hlLmJlYW0uc2RrLnZhbHVlcy5QQ29sbGVjdGlvblZpZXdzJFNpbmdsZXRvblZpZXdGbgOFtTzTeS1TAgADWgAKaGFzRGVmYXVsdFsAE2VuY29kZWREZWZhdWx0VmFsdWV0AAJbQkwACnZhbHVlQ29kZXJxAH4AMnhyACVvcmcuYXBhY2hlLmJlYW0uc2RrLnRyYW5zZm9ybXMuVmlld0ZuPfqDl8/dAnkCAAB4cAF1cgACW0Ks8xf4BghU4AIAAHhwAAAACoCAgICAgICAgAFxAH4A9HNyAD9vcmcuYXBhY2hlLmJlYW0uc2RrLnRyYW5zZm9ybXMud2luZG93aW5nLlBhcnRpdGlvbmluZ1dpbmRvd0ZuJDHY+f5kaHgrtgIAAUwABnRoaXMkMHQAP0xvcmcvYXBhY2hlL2JlYW0vc2RrL3RyYW5zZm9ybXMvd2luZG93aW5nL1BhcnRpdGlvbmluZ1dpbmRvd0ZuO3hyADhvcmcuYXBhY2hlLmJlYW0uc2RrLnRyYW5zZm9ybXMud2luZG93aW5nLldpbmRvd01hcHBpbmdGbj8ywMYTW2w2AgABTAAPbWF4aW11bUxvb2tiYWNrdAAYTG9yZy9qb2RhL3RpbWUvRHVyYXRpb247eHBzcgAWb3JnLmpvZGEudGltZS5EdXJhdGlvbgAAAj96Uc7WAgAAeHIAH29yZy5qb2RhLnRpbWUuYmFzZS5CYXNlRHVyYXRpb24AAAJZGTr0jgIAAUoAB2lNaWxsaXN4cAAAAAAAAAAAc3IANW9yZy5hcGFjaGUuYmVhbS5zZGsudHJhbnNmb3Jtcy53aW5kb3dpbmcuRml4ZWRXaW5kb3dzBGk/tu2/O4gCAAJMAAZvZmZzZXRxAH4BAUwABHNpemVxAH4BAXhyAD1vcmcuYXBhY2hlLmJlYW0uc2RrLnRyYW5zZm9ybXMud2luZG93aW5nLlBhcnRpdGlvbmluZ1dpbmRvd0ZubJoylYyQAIMCAAB4cgA7b3JnLmFwYWNoZS5iZWFtLnNkay50cmFuc2Zvcm1zLndpbmRvd2luZy5Ob25NZXJnaW5nV2luZG93Rm5XBgtn0+6oqwIAAHhyADFvcmcuYXBhY2hlLmJlYW0uc2RrLnRyYW5zZm9ybXMud2luZG93aW5nLldpbmRvd0ZuxgQZebqKllcCAAB4cHEAfgEFc3EAfgEDAAAAAAAAdTBzcgAsb3JnLmFwYWNoZS5iZWFtLnNkay52YWx1ZXMuV2luZG93aW5nU3RyYXRlZ3mkTWYkLswV5QIAC1oAGGFsbG93ZWRMYXRlbmVzc1NwZWNpZmllZFoADW1vZGVTcGVjaWZpZWRaABp0aW1lc3RhbXBDb21iaW5lclNwZWNpZmllZFoAEHRyaWdnZXJTcGVjaWZpZWRMAA9hbGxvd2VkTGF0ZW5lc3NxAH4BAUwAD2Nsb3NpbmdCZWhhdmlvcnQAQUxvcmcvYXBhY2hlL2JlYW0vc2RrL3RyYW5zZm9ybXMvd2luZG93aW5nL1dpbmRvdyRDbG9zaW5nQmVoYXZpb3I7TAAEbW9kZXQAP0xvcmcvYXBhY2hlL2JlYW0vc2RrL3ZhbHVlcy9XaW5kb3dpbmdTdHJhdGVneSRBY2N1bXVsYXRpb25Nb2RlO0wADm9uVGltZUJlaGF2aW9ydABATG9yZy9hcGFjaGUvYmVhbS9zZGsvdHJhbnNmb3Jtcy93aW5kb3dpbmcvV2luZG93JE9uVGltZUJlaGF2aW9yO0wAEXRpbWVzdGFtcENvbWJpbmVydAA8TG9yZy9hcGFjaGUvYmVhbS9zZGsvdHJhbnNmb3Jtcy93aW5kb3dpbmcvVGltZXN0YW1wQ29tYmluZXI7TAAHdHJpZ2dlcnQAMkxvcmcvYXBhY2hlL2JlYW0vc2RrL3RyYW5zZm9ybXMvd2luZG93aW5nL1RyaWdnZXI7TAAId2luZG93Rm50ADNMb3JnL2FwYWNoZS9iZWFtL3Nkay90cmFuc2Zvcm1zL3dpbmRvd2luZy9XaW5kb3dGbjt4cAAAAABxAH4BBX5yAD9vcmcuYXBhY2hlLmJlYW0uc2RrLnRyYW5zZm9ybXMud2luZG93aW5nLldpbmRvdyRDbG9zaW5nQmVoYXZpb3IAAAAAAAAAABIAAHhxAH4AYHQAEUZJUkVfSUZfTk9OX0VNUFRZfnIAPW9yZy5hcGFjaGUuYmVhbS5zZGsudmFsdWVzLldpbmRvd2luZ1N0cmF0ZWd5JEFjY3VtdWxhdGlvbk1vZGUAAAAAAAAAABIAAHhxAH4AYHQAFkRJU0NBUkRJTkdfRklSRURfUEFORVN+cgA+b3JnLmFwYWNoZS5iZWFtLnNkay50cmFuc2Zvcm1zLndpbmRvd2luZy5XaW5kb3ckT25UaW1lQmVoYXZpb3IAAAAAAAAAABIAAHhxAH4AYHQAC0ZJUkVfQUxXQVlTfnIAOm9yZy5hcGFjaGUuYmVhbS5zZGsudHJhbnNmb3Jtcy53aW5kb3dpbmcuVGltZXN0YW1wQ29tYmluZXIAAAAAAAAAABIAAHhxAH4AYHQADUVORF9PRl9XSU5ET1dzcgA3b3JnLmFwYWNoZS5iZWFtLnNkay50cmFuc2Zvcm1zLndpbmRvd2luZy5EZWZhdWx0VHJpZ2dlcrEK44fRvTpzAgAAeHIAMG9yZy5hcGFjaGUuYmVhbS5zZGsudHJhbnNmb3Jtcy53aW5kb3dpbmcuVHJpZ2dlcjZNMyF5+kEPAgABTAALc3ViVHJpZ2dlcnNxAH4ALnhwcQB+ALFxAH4BCnNyACdvcmcuYXBhY2hlLmJlYW0uc2RrLm5leG1hcmsubW9kZWwuQmlkJDGAPP2LGdg4zwIAAHhyACZvcmcuYXBhY2hlLmJlYW0uc2RrLmNvZGVycy5DdXN0b21Db2RlcmqwCJ0LOx0LAgAAeHEAfgDuc3EAfgD1AHQABm91dHB1dHNxAH4ADj9AAAAAAAAMdwgAAAAQAAAAAXEAfgEmcQB+ASV4c3IARW9yZy5hcGFjaGUuYmVhbS5ydW5uZXJzLmNvcmUuY29uc3RydWN0aW9uLlNlcmlhbGl6YWJsZVBpcGVsaW5lT3B0aW9uc9xu0W/W1iD7AgABTAAZc2VyaWFsaXplZFBpcGVsaW5lT3B0aW9uc3EAfgAFeHB0C2l7Im9wdGlvbnMiOnsiYXNzZXJ0Q29ycmVjdG5lc3MiOmZhbHNlLCJvdXRPZk9yZGVyR3JvdXBTaXplIjpudWxsLCJ1c2VQdWJzdWJQdWJsaXNoVGltZSI6bnVsbCwibWFuYWdlUmVzb3VyY2VzIjpmYWxzZSwiYXZnUGVyc29uQnl0ZVNpemUiOm51bGwsInJhdGVTaGFwZSI6bnVsbCwib2NjYXNpb25hbERlbGF5U2VjIjpudWxsLCJydW5uZXIiOiJvcmcuYXBhY2hlLm5lbW8uY29tcGlsZXIuZnJvbnRlbmQuYmVhbS5OZW1vUnVubmVyIiwiZGlza0J1c3lCeXRlcyI6bnVsbCwiY3B1RGVsYXlNcyI6bnVsbCwiaXNSYXRlTGltaXRlZCI6dHJ1ZSwiaG90U2VsbGVyc1JhdGlvIjpudWxsLCJzdHJlYW1UaW1lb3V0Ijo0MDAsIm9wdGlvbnNJZCI6MCwicXVlcnkiOjcsImF1Y3Rpb25Ta2lwIjpudWxsLCJyYXRlVW5pdCI6bnVsbCwibnVtRXZlbnRHZW5lcmF0b3JzIjoxLCJyYXRlUGVyaW9kU2VjIjpudWxsLCJ1c2VXYWxsY2xvY2tFdmVudFRpbWUiOm51bGwsInNvdXJjZVR5cGUiOm51bGwsInB1YlN1Yk1vZGUiOm51bGwsInF1ZXJ5TGFuZ3VhZ2UiOm51bGwsInNpbmtUeXBlIjpudWxsLCJob3RBdWN0aW9uUmF0aW8iOm51bGwsImZpcnN0RXZlbnRSYXRlIjoxMDAsImV4cG9ydFN1bW1hcnlUb0JpZ1F1ZXJ5IjpmYWxzZSwiZmFub3V0IjoxLCJtYXhMb2dFdmVudHMiOm51bGwsImhvdEJpZGRlcnNSYXRpbyI6bnVsbCwiYmFzZWxpbmVGaWxlbmFtZSI6bnVsbCwibmV4dEV2ZW50UmF0ZSI6MTAwLCJ3aW5kb3dTaXplU2VjIjozMCwiYXZnQXVjdGlvbkJ5dGVTaXplIjpudWxsLCJzdHJlYW1pbmciOnRydWUsInByb2JEZWxheWVkRXZlbnQiOm51bGwsImF2Z0JpZEJ5dGVTaXplIjpudWxsLCJtYXhBdWN0aW9uc1dhaXRpbmdUaW1lIjpudWxsLCJjb2RlclN0cmF0ZWd5IjpudWxsLCJqdXN0TW9kZWxSZXN1bHRSYXRlIjpmYWxzZSwibnVtQWN0aXZlUGVvcGxlIjpudWxsLCJwcmVsb2FkU2Vjb25kcyI6bnVsbCwiZGVidWciOm51bGwsImFwcE5hbWUiOiJNYWluIiwibnVtSW5GbGlnaHRBdWN0aW9ucyI6bnVsbCwid2luZG93UGVyaW9kU2VjIjo1LCJtb25pdG9ySm9icyI6dHJ1ZSwic3RhYmxlVW5pcXVlTmFtZXMiOiJXQVJOSU5HIiwibG9nRXZlbnRzIjpmYWxzZSwic3VpdGUiOiJERUZBVUxUIiwibG9nUmVzdWx0cyI6ZmFsc2UsIm51bUV2ZW50cyI6NDAwMDAwMDAsIndhdGVybWFya0hvbGRiYWNrU2VjIjpudWxsfSwiZGlzcGxheV9kYXRhIjpbeyJuYW1lc3BhY2UiOiJvcmcuYXBhY2hlLmJlYW0uc2RrLm9wdGlvbnMuQXBwbGljYXRpb25OYW1lT3B0aW9ucyIsImtleSI6ImFwcE5hbWUiLCJ0eXBlIjoiU1RSSU5HIiwidmFsdWUiOiJNYWluIn0seyJuYW1lc3BhY2UiOiJvcmcuYXBhY2hlLmJlYW0uc2RrLm5leG1hcmsuTmV4bWFya09wdGlvbnMiLCJrZXkiOiJtb25pdG9ySm9icyIsInR5cGUiOiJCT09MRUFOIiwidmFsdWUiOnRydWV9LHsibmFtZXNwYWNlIjoib3JnLmFwYWNoZS5iZWFtLnNkay5vcHRpb25zLlBpcGVsaW5lT3B0aW9ucyIsImtleSI6InJ1bm5lciIsInR5cGUiOiJKQVZBX0NMQVNTIiwidmFsdWUiOiJvcmcuYXBhY2hlLm5lbW8uY29tcGlsZXIuZnJvbnRlbmQuYmVhbS5OZW1vUnVubmVyIiwic2hvcnRWYWx1ZSI6Ik5lbW9SdW5uZXIifSx7Im5hbWVzcGFjZSI6Im9yZy5hcGFjaGUuYmVhbS5zZGsubmV4bWFyay5OZXhtYXJrT3B0aW9ucyIsImtleSI6Im51bUV2ZW50cyIsInR5cGUiOiJJTlRFR0VSIiwidmFsdWUiOjQwMDAwMDAwfSx7Im5hbWVzcGFjZSI6Im9yZy5hcGFjaGUuYmVhbS5zZGsubmV4bWFyay5OZXhtYXJrT3B0aW9ucyIsImtleSI6InF1ZXJ5IiwidHlwZSI6IklOVEVHRVIiLCJ2YWx1ZSI6N30seyJuYW1lc3BhY2UiOiJvcmcuYXBhY2hlLmJlYW0uc2RrLm9wdGlvbnMuU3RyZWFtaW5nT3B0aW9ucyIsImtleSI6InN0cmVhbWluZyIsInR5cGUiOiJCT09MRUFOIiwidmFsdWUiOnRydWV9LHsibmFtZXNwYWNlIjoib3JnLmFwYWNoZS5iZWFtLnNkay5uZXhtYXJrLk5leG1hcmtPcHRpb25zIiwia2V5Ijoic3RyZWFtVGltZW91dCIsInR5cGUiOiJJTlRFR0VSIiwidmFsdWUiOjQwMH0seyJuYW1lc3BhY2UiOiJvcmcuYXBhY2hlLmJlYW0uc2RrLm5leG1hcmsuTmV4bWFya09wdGlvbnMiLCJrZXkiOiJmYW5vdXQiLCJ0eXBlIjoiSU5URUdFUiIsInZhbHVlIjoxfSx7Im5hbWVzcGFjZSI6Im9yZy5hcGFjaGUuYmVhbS5zZGsubmV4bWFyay5OZXhtYXJrT3B0aW9ucyIsImtleSI6ImlzUmF0ZUxpbWl0ZWQiLCJ0eXBlIjoiQk9PTEVBTiIsInZhbHVlIjp0cnVlfSx7Im5hbWVzcGFjZSI6Im9yZy5hcGFjaGUuYmVhbS5zZGsubmV4bWFyay5OZXhtYXJrT3B0aW9ucyIsImtleSI6ImZpcnN0RXZlbnRSYXRlIiwidHlwZSI6IklOVEVHRVIiLCJ2YWx1ZSI6MTAwfSx7Im5hbWVzcGFjZSI6Im9yZy5hcGFjaGUuYmVhbS5zZGsubmV4bWFyay5OZXhtYXJrT3B0aW9ucyIsImtleSI6Im1hbmFnZVJlc291cmNlcyIsInR5cGUiOiJCT09MRUFOIiwidmFsdWUiOmZhbHNlfSx7Im5hbWVzcGFjZSI6Im9yZy5hcGFjaGUuYmVhbS5zZGsubmV4bWFyay5OZXhtYXJrT3B0aW9ucyIsImtleSI6IndpbmRvd1BlcmlvZFNlYyIsInR5cGUiOiJJTlRFR0VSIiwidmFsdWUiOjV9LHsibmFtZXNwYWNlIjoib3JnLmFwYWNoZS5iZWFtLnNkay5uZXhtYXJrLk5leG1hcmtPcHRpb25zIiwia2V5Ijoid2luZG93U2l6ZVNlYyIsInR5cGUiOiJJTlRFR0VSIiwidmFsdWUiOjMwfSx7Im5hbWVzcGFjZSI6Im9yZy5hcGFjaGUuYmVhbS5zZGsubmV4bWFyay5OZXhtYXJrT3B0aW9ucyIsImtleSI6Im51bUV2ZW50R2VuZXJhdG9ycyIsInR5cGUiOiJJTlRFR0VSIiwidmFsdWUiOjF9LHsibmFtZXNwYWNlIjoib3JnLmFwYWNoZS5iZWFtLnNkay5uZXhtYXJrLk5leG1hcmtPcHRpb25zIiwia2V5IjoibmV4dEV2ZW50UmF0ZSIsInR5cGUiOiJJTlRFR0VSIiwidmFsdWUiOjEwMH1dfXNxAH4ADj9AAAAAAAAMdwgAAAAQAAAAAXNxAH4AGwAAAABxAH4A63hxAH4BE4AAAAAAAAAAgAAAAAAAAAB//////////3NxAH4AOwAAAAB3BAAAAAB4";

  /**
   * Netty event loop group for client worker.
   */
  private EventLoopGroup clientWorkerGroup;

  /**
   * Netty client bootstrap.
   */
  private Bootstrap clientBootstrap;

  private final ConcurrentMap<Channel, EventHandler<NemoEvent>> map;

	private void createClassLoader() {
		// read jar file
		final S3Object result = s3Client.getObject(BUCKET_NAME, "jars/nexmark-0.2-SNAPSHOT-shaded.jar");
		//final S3Object result = s3Client.getObject(BUCKET_NAME, "jars/shaded.jar");
		if (!Files.exists(Paths.get(PATH))) {
			LOG.info("Copying file...");
			final InputStream in = result.getObjectContent();
			try {
				Files.copy(in, Paths.get(PATH));
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}

		try {
			final URL[] urls = new URL[1];
			final File f = new File(PATH);
      urls[0] = f.toURI().toURL();
			LOG.info("File: {}, {}", f.toPath(), urls[0]);
			classLoader = new URLClassLoader(urls, this.getClass().getClassLoader());
		} catch (MalformedURLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
  }

	public HelloNettyHandler() {
		LOG.info("Handler is created! {}", this);
          this.clientWorkerGroup = new NioEventLoopGroup(1,
        new DefaultThreadFactory("hello" + "-ClientWorker"));
    this.clientBootstrap = new Bootstrap();
    this.map = new ConcurrentHashMap<>();
    this.clientBootstrap.group(clientWorkerGroup)
        .channel(NioSocketChannel.class)
        .handler(new NettyChannelInitializer(new NettyLambdaInboundHandler(map)))
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.SO_KEEPALIVE, true);
	}

	@Override
	public Object handleRequest(Map<String, Object> input, Context context) {
		System.out.println("Input: " + input);

		if (classLoader == null) {
			createClassLoader();
			irVertex = SerializeUtils.deserializeFromString(serializedCodeQuery7, classLoader);
			LOG.info("Create class loader: {}", classLoader);
		}


		final List<String> result = new ArrayList<>();
    final OutputCollector outputCollector = new LambdaOutputHandler(result);

		irVertex.getTransform().prepare(new LambdaRuntimeContext(irVertex), outputCollector);

		if (input.isEmpty()) {
		  // this is warmer, just return;
      System.out.println("Warm up");
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return null;
    }

    boolean channelOpen = false;
    Channel opendChannel = null;
    for (final Map.Entry<Channel, EventHandler<NemoEvent>> entry : map.entrySet()) {
      final Channel channel = entry.getKey();
      if (!channel.isOpen()) {
        channel.close();
        map.remove(channel);
      } else {
        opendChannel = channel;
        break;
      }
    }


    if (opendChannel == null) {
      final String address = (String) input.get("address");
      final Integer port = (Integer) input.get("port");

      final ChannelFuture channelFuture;
      channelFuture = clientBootstrap.connect(new InetSocketAddress(address, port));
      channelFuture.awaitUninterruptibly();
      assert channelFuture.isDone();
      if (!channelFuture.isSuccess()) {
        final StringBuilder sb = new StringBuilder("A connection failed at Source - ");
        sb.append(channelFuture.cause());
        throw new RuntimeException(sb.toString());
      }
      opendChannel = channelFuture.channel();
      map.put(opendChannel, new LambdaEventHandler(outputCollector, opendChannel));
    }

    System.out.println("Open channel: " + opendChannel);

    // client handshake
    opendChannel.writeAndFlush(new NemoEvent(NemoEvent.Type.CLIENT_HANDSHAKE, new byte[0], 0));
    System.out.println("Write handshake");

    final LambdaEventHandler handler = (LambdaEventHandler) map.get(opendChannel);
    try {
      // wait until end
      System.out.println("Wait end flag");
      final Integer endFlag = handler.endBlockingQueue.take();

      // send result
      final byte[] bytes = result.toString().getBytes();
      final ChannelFuture future =
        opendChannel.writeAndFlush(
          new NemoEvent(NemoEvent.Type.RESULT, bytes, bytes.length));
      future.get();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    //final Decoder object  = (T)ois.readObject();
		//ois.close();
		//return object;

    return null;
    //return result.toString();

	}

  final class LambdaEventHandler implements EventHandler<NemoEvent> {

    private WindowedValue sideInput;
    private LambdaDecoderFactory sideInputDecoderFactory;
    private LambdaDecoderFactory mainInputDecoderFactory;

    private final OutputCollector outputCollector;

    private final BlockingQueue<Integer> endBlockingQueue = new LinkedBlockingQueue<>();
    private final Channel opendChannel;

    public LambdaEventHandler(final OutputCollector outputCollector,
                              final Channel opendChannel) {
      this.outputCollector = outputCollector;
      this.opendChannel = opendChannel;
    }

    @Override
    public synchronized void onNext(final NemoEvent nemoEvent) {
      switch (nemoEvent.getType()) {
        case SIDE: {
          // receive side input
          System.out.println("Receive side");
          final ByteArrayInputStream bis = new ByteArrayInputStream(nemoEvent.getBytes());
          sideInputDecoderFactory =
            SerializeUtils.deserialize(bis, classLoader);
          try {
            final DecoderFactory.Decoder sideInputDecoder = sideInputDecoderFactory.create(bis);
            sideInput = (WindowedValue) sideInputDecoder.decode();
            irVertex.getTransform().onData(sideInput);
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
          break;
        }
        case MAIN:
          System.out.println("Receive main ");
          // receive main input
          if (sideInput == null) {
            throw new IllegalStateException("SideInput should not be null");
          }

          final ByteArrayInputStream bis = new ByteArrayInputStream(nemoEvent.getBytes());
          mainInputDecoderFactory =
            SerializeUtils.deserialize(bis, classLoader);
          try {
            final DecoderFactory.Decoder mainInputDecoder = mainInputDecoderFactory.create(bis);
            WindowedValue mainInput = null;
            int cnt = 0;
            while (true) {
              try {
                mainInput = (WindowedValue) mainInputDecoder.decode();
                //handler.processMainAndSideInput(mainInput, sideInput, outputCollector);
                irVertex.getTransform().onData(mainInput);
                cnt += 1;
              } catch (final IOException e) {
                if(e.getMessage().contains("EOF")) {
                  System.out.println("Cnt: " + cnt + ", eof!");
                } else {
                  System.out.println("Cnt: " + cnt + "Windowed value: " + mainInput + ", sideInput: " + sideInput + ", oc: " + outputCollector);
                  throw e;
                }
                break;
              }
            }

            sideInput = null;
            endBlockingQueue.add(1);
          } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
          break;
        case END:
          // end of event
          // update handler
          break;
      }
    }
  }

	final class LambdaOutputHandler  implements OutputCollector {

	  private final List<String> result;

	  public LambdaOutputHandler(final List<String> result) {
	    this.result = result;
    }

    @Override
    public void emit(Object output) {
      System.out.println("Emit output: " + output);
      result.add(output.toString());
    }

    @Override
    public void emitWatermark(Watermark watermark) {

    }

    @Override
    public void emit(String dstVertexId, Object output) {

    }
  }
}
