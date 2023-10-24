import { Kafka } from "@upstash/kafka";

export default {
	async fetch(request, env, ctx) {
		// auth check
		if (request.method != 'POST') {
			return new Response("no auth", { status: 403 });
		}
		const param = JSON.parse(await request.text());
		if (param.pass != env.PASS) {
			return new Response(param, { status: 403 });
		}
		const kafka = new Kafka({
			url: env.UPSTASH_KAFKA_REST_URL,
			username: env.UPSTASH_KAFKA_REST_USERNAME,
			password: env.UPSTASH_KAFKA_REST_PASSWORD,
		});
		const topic = "sms";
		if (new URL(request.url).pathname == "/send") {
			// send msg
			const msg = param.msg;
			const p = kafka.producer();
			ctx.waitUntil(p.produce(topic, msg));
			const data = {
				code: 200,
				msg: "ok!"
			}
			return new Response(JSON.stringify(data), { status: 200 });
		} else if (new URL(request.url).pathname == "/get") {
			// get msg
			const c = kafka.consumer();
			const messages = await c.consume({
				consumerGroupId: "group_1",
				instanceId: "instance_1",
				topics: ["sms"],
				autoOffsetReset: "earliest",
			});
			// return new Response(JSON.stringify(messages, null, 2));
			const data = messages.map(item => {
				const parts = item.value.split("\n");
				const o = {
					sender: parts[0],
					time: parts[1],
					msg: parts.slice(2).join("\n")
				};
				return o;
			});
		
			return new Response(JSON.stringify(data));
		}
		else {
			return new Response("no auth", { status: 403 });
		}
	},
};