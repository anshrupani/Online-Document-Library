FROM alpine:latest
MAINTAINER anshrupani18
RUN apk update
RUN apk --no-cache add nginx 
RUN mkdir -pv /run/nginx
COPY nginx.conf /etc/nginx/
EXPOSE 80
CMD ["nginx"]
